import type { IncomingMessage, ServerResponse } from "node:http";
import { safeEqualSecret } from "openclaw/plugin-sdk/security-runtime";
import { normalizeLowercaseStringOrEmpty } from "openclaw/plugin-sdk/string-coerce-runtime";
import type { PluginRuntime } from "../api.js";
import {
  createFixedWindowRateLimiter,
  createWebhookInFlightLimiter,
  readJsonWebhookBodyOrReject,
  resolveRequestClientIp,
  resolveWebhookTargetWithAuthOrRejectSync,
  withResolvedWebhookRequestPipeline,
  WEBHOOK_IN_FLIGHT_DEFAULTS,
  WEBHOOK_RATE_LIMIT_DEFAULTS,
  type OpenClawConfig,
  type WebhookInFlightLimiter,
} from "../runtime-api.js";
import type { WebhookSecretInput } from "./config.js";
import { formatZodError, webhookActionSchema, type WebhookAction } from "./http-request-schema.js";

type BoundTaskFlowRuntime = ReturnType<
  PluginRuntime["tasks"]["async"]["managedFlows"]["bindSession"]
> &
  Pick<ReturnType<PluginRuntime["tasks"]["managedFlows"]["bindSession"]>, "cancel">;

export type TaskFlowWebhookTarget = {
  routeId: string;
  path: string;
  secretInput: WebhookSecretInput;
  defaultControllerId: string;
  taskFlow: BoundTaskFlowRuntime;
};

type FlowRecord = NonNullable<Awaited<ReturnType<BoundTaskFlowRuntime["get"]>>>;
type TaskRecord = NonNullable<Awaited<ReturnType<BoundTaskFlowRuntime["cancel"]>>["tasks"]>[number];
type FlowMutationResult = Awaited<ReturnType<BoundTaskFlowRuntime["setWaiting"]>>;
type WebhookOutcome = {
  statusCode: number;
  code?: string;
  error?: string;
  result: unknown;
};

function pickOptionalFields<T extends object, TKey extends keyof T & string>(
  source: T,
  keys: readonly TKey[],
): Partial<Pick<T, TKey>> {
  const result: Partial<Pick<T, TKey>> = {};
  for (const key of keys) {
    const value = source[key];
    if (value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}

function pickOptionalTruthyStringFields<T extends object, TKey extends keyof T & string>(
  source: T,
  keys: readonly TKey[],
): Partial<Pick<T, TKey>> {
  const result: Partial<Pick<T, TKey>> = {};
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string" && value) {
      result[key] = value;
    }
  }
  return result;
}

function toFlowView(flow: FlowRecord) {
  return {
    flowId: flow.flowId,
    syncMode: flow.syncMode,
    ...pickOptionalTruthyStringFields(flow, [
      "controllerId",
      "currentStep",
      "blockedTaskId",
      "blockedSummary",
    ]),
    revision: flow.revision,
    status: flow.status,
    notifyPolicy: flow.notifyPolicy,
    goal: flow.goal,
    ...pickOptionalFields(flow, ["stateJson", "waitJson", "cancelRequestedAt"]),
    createdAt: flow.createdAt,
    updatedAt: flow.updatedAt,
    ...pickOptionalFields(flow, ["endedAt"]),
  };
}

function toTaskView(task: TaskRecord) {
  return {
    taskId: task.taskId,
    runtime: task.runtime,
    ...pickOptionalTruthyStringFields(task, [
      "sourceId",
      "childSessionKey",
      "parentFlowId",
      "parentTaskId",
      "agentId",
      "runId",
      "label",
      "error",
      "progressSummary",
      "terminalSummary",
      "terminalOutcome",
    ]),
    scopeKind: task.scopeKind,
    task: task.task,
    status: task.status,
    deliveryStatus: task.deliveryStatus,
    notifyPolicy: task.notifyPolicy,
    createdAt: task.createdAt,
    ...pickOptionalFields(task, ["startedAt", "endedAt", "lastEventAt", "cleanupAfter"]),
  };
}

function writeJson(res: ServerResponse, statusCode: number, body: unknown): void {
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

function extractSharedSecret(req: IncomingMessage): string {
  const authHeader = Array.isArray(req.headers.authorization)
    ? (req.headers.authorization[0] ?? "")
    : (req.headers.authorization ?? "");
  if (normalizeLowercaseStringOrEmpty(authHeader).startsWith("bearer ")) {
    return authHeader.slice("bearer ".length).trim();
  }
  const sharedHeader = req.headers["x-openclaw-webhook-secret"];
  return Array.isArray(sharedHeader) ? (sharedHeader[0] ?? "").trim() : (sharedHeader ?? "").trim();
}

function mapFlowMutationResult(result: FlowMutationResult): WebhookOutcome {
  return {
    ...mapMutationStatus(result),
    result: result.applied
      ? { applied: true, flow: toFlowView(result.flow) }
      : {
          applied: false,
          code: result.code,
          ...(result.current ? { current: toFlowView(result.current) } : {}),
        },
  };
}

function mapMutationStatus(result: {
  applied: boolean;
  code?: "not_found" | "not_managed" | "revision_conflict" | "persist_failed";
}): { statusCode: number; code?: string; error?: string } {
  if (result.applied) {
    return { statusCode: 200 };
  }
  switch (result.code) {
    case "not_found":
      return {
        statusCode: 404,
        code: "not_found",
        error: "TaskFlow not found.",
      };
    case "not_managed":
      return {
        statusCode: 409,
        code: "not_managed",
        error: "TaskFlow is not managed by this webhook surface.",
      };
    case "revision_conflict":
      return {
        statusCode: 409,
        code: "revision_conflict",
        error: "TaskFlow changed since the caller's expected revision.",
      };
    case "persist_failed":
      return {
        statusCode: 503,
        code: "persist_failed",
        error: "TaskFlow persistence failed.",
      };
    default:
      return {
        statusCode: 409,
        code: "mutation_rejected",
        error: "TaskFlow mutation was rejected.",
      };
  }
}

const operationRejectionCodes: Record<"run_task" | "cancel_flow", Record<string, string>> = {
  run_task: {
    "Flow cancellation has already been requested.": "cancel_requested",
    "Flow does not accept managed child tasks.": "not_managed",
    "Task persistence failed.": "persist_failed",
  },
  cancel_flow: {
    "One or more child tasks are still active.": "cancel_pending",
    "Flow changed while cancellation was in progress.": "revision_conflict",
    "Flow persistence failed.": "persist_failed",
  },
};

function mapOperationRejection(
  action: keyof typeof operationRejectionCodes,
  result: { found: boolean; reason?: string },
): Omit<WebhookOutcome, "result"> {
  if (!result.found) {
    return { statusCode: 404, code: "not_found", error: "TaskFlow not found." };
  }
  const codes = operationRejectionCodes[action];
  const code = result.reason?.startsWith("Flow is already ")
    ? "terminal"
    : result.reason !== undefined && Object.hasOwn(codes, result.reason)
      ? codes[result.reason]
      : action === "run_task"
        ? "task_not_created"
        : "cancel_rejected";
  return {
    statusCode: code === "cancel_pending" ? 202 : code === "persist_failed" ? 503 : 409,
    code,
    error:
      result.reason ??
      (action === "run_task"
        ? "TaskFlow task was not created."
        : "TaskFlow cancellation was rejected."),
  };
}

async function executeWebhookAction(params: {
  action: WebhookAction;
  target: TaskFlowWebhookTarget;
  cfg: OpenClawConfig;
}): Promise<WebhookOutcome> {
  const { action, target } = params;
  switch (action.action) {
    case "create_flow": {
      const { action: _action, ...input } = action;
      const flow = await target.taskFlow.tryCreateManaged({
        ...input,
        controllerId: input.controllerId ?? target.defaultControllerId,
        currentStep: input.currentStep ?? undefined,
      });
      return flow
        ? { statusCode: 200, result: { created: true, flow: toFlowView(flow) } }
        : {
            statusCode: 503,
            code: "persist_failed",
            error: "TaskFlow persistence failed.",
            result: { created: false, code: "persist_failed" },
          };
    }
    case "get_flow": {
      const flow = await target.taskFlow.get(action.flowId);
      return { statusCode: 200, result: { flow: flow ? toFlowView(flow) : null } };
    }
    case "list_flows":
      return { statusCode: 200, result: { flows: (await target.taskFlow.list()).map(toFlowView) } };
    case "find_latest_flow": {
      const flow = await target.taskFlow.findLatest();
      return { statusCode: 200, result: { flow: flow ? toFlowView(flow) : null } };
    }
    case "resolve_flow": {
      const flow = await target.taskFlow.resolve(action.token);
      return { statusCode: 200, result: { flow: flow ? toFlowView(flow) : null } };
    }
    case "get_task_summary":
      return {
        statusCode: 200,
        result: { summary: (await target.taskFlow.getTaskSummary(action.flowId)) ?? null },
      };
    case "set_waiting": {
      const { action: _action, ...input } = action;
      return mapFlowMutationResult(await target.taskFlow.setWaiting(input));
    }
    case "resume_flow": {
      const { action: _action, ...input } = action;
      return mapFlowMutationResult(await target.taskFlow.resume(input));
    }
    case "finish_flow": {
      const { action: _action, ...input } = action;
      return mapFlowMutationResult(await target.taskFlow.finish(input));
    }
    case "fail_flow": {
      const { action: _action, ...input } = action;
      return mapFlowMutationResult(await target.taskFlow.fail(input));
    }
    case "request_cancel": {
      const { action: _action, ...input } = action;
      return mapFlowMutationResult(await target.taskFlow.requestCancel(input));
    }
    case "cancel_flow": {
      const result = await target.taskFlow.cancel({
        flowId: action.flowId,
        cfg: params.cfg,
      });
      const projected = {
        found: result.found,
        cancelled: result.cancelled,
        ...(result.reason ? { reason: result.reason } : {}),
        ...(result.flow ? { flow: toFlowView(result.flow) } : {}),
        ...(result.tasks ? { tasks: result.tasks.map(toTaskView) } : {}),
      };
      return {
        ...(projected.cancelled
          ? { statusCode: 200 }
          : mapOperationRejection("cancel_flow", projected)),
        result: projected,
      };
    }
    case "run_task": {
      const { action: _action, ...input } = action;
      const result = await target.taskFlow.runTask(input);
      return {
        ...(result.created ? { statusCode: 200 } : mapOperationRejection("run_task", result)),
        result: result.created
          ? { created: true, flow: toFlowView(result.flow), task: toTaskView(result.task) }
          : {
              found: result.found,
              created: false,
              reason: result.reason,
              ...(result.flow ? { flow: toFlowView(result.flow) } : {}),
            },
      };
    }
  }
  throw new Error("Unsupported webhook action");
}

export function createTaskFlowWebhookRequestHandler(params: {
  cfg: OpenClawConfig;
  targetsByPath: Map<string, TaskFlowWebhookTarget[]>;
  inFlightLimiter?: WebhookInFlightLimiter;
}): (req: IncomingMessage, res: ServerResponse) => Promise<boolean> {
  const rateLimiter = createFixedWindowRateLimiter({
    windowMs: WEBHOOK_RATE_LIMIT_DEFAULTS.windowMs,
    maxRequests: WEBHOOK_RATE_LIMIT_DEFAULTS.maxRequests,
    maxTrackedKeys: WEBHOOK_RATE_LIMIT_DEFAULTS.maxTrackedKeys,
  });
  const inFlightLimiter =
    params.inFlightLimiter ??
    createWebhookInFlightLimiter({
      maxInFlightPerKey: WEBHOOK_IN_FLIGHT_DEFAULTS.maxInFlightPerKey,
      maxTrackedKeys: WEBHOOK_IN_FLIGHT_DEFAULTS.maxTrackedKeys,
    });
  return async (req: IncomingMessage, res: ServerResponse): Promise<boolean> => {
    return await withResolvedWebhookRequestPipeline({
      req,
      res,
      targetsByPath: params.targetsByPath,
      allowMethods: ["POST"],
      requireJsonContentType: true,
      rateLimiter,
      rateLimitKey: (() => {
        const clientIp =
          resolveRequestClientIp(
            req,
            params.cfg.gateway?.trustedProxies,
            params.cfg.gateway?.allowRealIpFallback === true,
          ) ??
          req.socket.remoteAddress ??
          "unknown";
        return `${new URL(req.url ?? "/", "http://localhost").pathname}:${clientIp}`;
      })(),
      inFlightLimiter,
      handle: async ({ targets }) => {
        const presentedSecret = extractSharedSecret(req);
        const target = resolveWebhookTargetWithAuthOrRejectSync({
          targets,
          res,
          isMatch: (candidate) => {
            if (presentedSecret.length === 0) {
              return false;
            }
            return (
              typeof candidate.secretInput === "string" &&
              safeEqualSecret(candidate.secretInput, presentedSecret)
            );
          },
        });
        if (!target) {
          return true;
        }

        const body = await readJsonWebhookBodyOrReject({
          req,
          res,
          maxBytes: 256 * 1024,
          timeoutMs: 15_000,
          emptyObjectOnEmpty: false,
          invalidJsonMessage: "invalid request body",
        });
        if (!body.ok) {
          return true;
        }

        const parsed = webhookActionSchema.safeParse(body.value);
        if (!parsed.success) {
          writeJson(res, 400, {
            ok: false,
            code: "invalid_request",
            error: formatZodError(parsed.error),
          });
          return true;
        }

        const { result, ...outcome } = await executeWebhookAction({
          action: parsed.data,
          target,
          cfg: params.cfg,
        });
        writeJson(
          res,
          outcome.statusCode,
          outcome.statusCode < 400
            ? {
                ok: true,
                routeId: target.routeId,
                ...(outcome.code ? { code: outcome.code } : {}),
                result,
              }
            : {
                ok: false,
                routeId: target.routeId,
                code: outcome.code ?? "request_rejected",
                error: outcome.error ?? "request rejected",
                result,
              },
        );
        return true;
      },
    });
  };
}
