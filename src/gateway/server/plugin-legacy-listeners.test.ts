import { once } from "node:events";
import { request, type Server } from "node:http";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { getWebhookLegacyListener } from "../../plugin-sdk/webhook-ingress.js";
import { readRequestBodyWithLimit } from "../../plugin-sdk/webhook-request-guards.js";
import { createPluginRuntimeCapabilityLease } from "../../plugins/capability-lease.js";
import {
  adoptPluginHttpRouteHandoffs,
  createPluginHttpRouteHandoff,
  registerPluginHttpRoute,
  withPluginHttpRouteRegistry,
} from "../../plugins/http-registry.js";
import { notifyPluginHttpRoutesChanged } from "../../plugins/http-route-owner.js";
import { createEmptyPluginRegistry } from "../../plugins/registry-empty.js";
import { getPluginRuntimeGatewayRequestScope } from "../../plugins/runtime/gateway-request-scope.js";
import {
  resetGatewayWorkAdmission,
  tryBeginGatewaySuspendAdmission,
} from "../../process/gateway-work-admission.js";
import { acquireTestPortBlock, type TestPortClaim } from "../../test-utils/port-claims.js";
import { createGatewayHttpServer } from "../server-http.js";
import { startPluginLegacyListeners } from "./plugin-legacy-listeners.js";
import { createGatewayPluginRequestHandler } from "./plugins-http.js";
import {
  isPluginAuthenticatedRoutePath,
  shouldEnforceGatewayAuthForPluginPath,
} from "./plugins-http/route-auth.js";

describe("legacy channel webhook ports", () => {
  let claim: TestPortClaim;
  let gatewayServer: Server;
  let stop: () => void;
  let registry = createEmptyPluginRegistry();
  const httpServers: Server[] = [];
  const cleanups: Array<() => void> = [];
  const warn = vi.fn();
  const url = (offset: number, path = "/webhook") =>
    `http://127.0.0.1:${claim.port + offset}${path}`;
  const endpoint = (offset: number) => ({ port: claim.port + offset, host: "127.0.0.1" });

  beforeAll(async () => {
    claim = await acquireTestPortBlock({ offsets: [0, 1, 2] });
    gatewayServer = createGatewayHttpServer({
      clients: new Set(),
      controlUiEnabled: false,
      controlUiBasePath: "/",
      handleHooksRequest: async () => false,
      resolvedAuth: { mode: "token", token: "synthetic-gateway-token", allowTailscale: false },
      getRuntimeConfig: () => ({ gateway: { trustedProxies: [] } }),
      handlePluginRequest: createGatewayPluginRequestHandler({
        registry,
        getRouteRegistry: () => registry,
        log: createSubsystemLogger("legacy-webhook-test"),
      }),
      shouldEnforcePluginGatewayAuth: (context) =>
        shouldEnforceGatewayAuthForPluginPath(registry, context),
      isPluginAuthenticatedRoute: (context) => isPluginAuthenticatedRoutePath(registry, context),
    });
    gatewayServer.listen(claim.port, "127.0.0.1");
    await once(gatewayServer, "listening");
    httpServers.push(gatewayServer);
    stop = startPluginLegacyListeners({
      gatewayServer,
      httpServers,
      getRegistry: () => registry,
      warn,
    });
  });

  afterEach(async () => {
    for (const cleanup of cleanups.splice(0).toReversed()) {
      cleanup();
    }
    registry = createEmptyPluginRegistry();
    const closed = httpServers.slice(1).map((server) => once(server, "close"));
    notifyPluginHttpRoutesChanged();
    await Promise.all(closed);
    resetGatewayWorkAdmission();
    warn.mockClear();
  });

  afterAll(async () => {
    stop();
    gatewayServer.closeAllConnections();
    await new Promise<void>((resolve) => {
      gatewayServer.close(() => resolve());
    });
    await claim.release();
  });

  const register = (params: Partial<Parameters<typeof registerPluginHttpRoute>[0]> = {}) => {
    const unregister = registerPluginHttpRoute({
      registry,
      path: "/webhook",
      pluginId: "demo",
      source: "webhook",
      auth: "plugin",
      reuseExistingSameOwner: true,
      handler: (_req, res) => {
        res.end("accepted");
      },
      ...params,
    });
    cleanups.push(unregister);
    return unregister;
  };

  const listening = async () => {
    // Registration publications coalesce in one microtask; socket readiness is event-driven.
    await Promise.resolve();
    await Promise.all(
      httpServers
        .slice(1)
        .filter((server) => !server.listening)
        .map((server) => once(server, "listening")),
    );
  };

  it("preserves raw bytes, peer identity, runtime scope, and Gateway admission without exposing other endpoints", async () => {
    const body = '{ "event": "synthetic", "spacing":  true }';
    register({
      legacyListener: endpoint(1),
      handler: async (req, res) => {
        if (req.headers["x-webhook-secret"] !== "synthetic-secret") {
          res.writeHead(401).end();
          return;
        }
        expect(await readRequestBodyWithLimit(req, { maxBytes: 1024, timeoutMs: 1000 })).toBe(body);
        res.setHeader("x-peer", req.socket.remoteAddress ?? "missing");
        const legacyListener = getWebhookLegacyListener(req);
        if (legacyListener) {
          expect(Reflect.set(legacyListener, "port", claim.port + 2)).toBe(false);
        }
        res.setHeader("x-legacy-listener", JSON.stringify(legacyListener ?? null));
        res.setHeader(
          "x-runtime-plugin",
          getPluginRuntimeGatewayRequestScope()?.pluginId ?? "missing",
        );
        res.end("accepted");
      },
    });
    register({ path: "/healthz", legacyListener: endpoint(1), handler: () => false });
    register({
      path: "/other",
      handler: (_req, res) => {
        res.end("other");
      },
    });
    await listening();
    for (const offset of [0, 1]) {
      const accepted = await fetch(url(offset), {
        method: "POST",
        body,
        headers: {
          "x-webhook-secret": "synthetic-secret",
          "x-openclaw-legacy-listener": JSON.stringify(endpoint(2)),
        },
      });
      expect(accepted.status).toBe(200);
      expect(await accepted.text()).toBe("accepted");
      expect(accepted.headers.get("x-peer")).toBe("127.0.0.1");
      expect(JSON.parse(accepted.headers.get("x-legacy-listener")!)).toEqual(
        offset === 0 ? null : endpoint(1),
      );
      expect(accepted.headers.get("x-runtime-plugin")).toBe("demo");
      expect((await fetch(url(offset), { method: "POST", body })).status).toBe(401);
    }
    expect((await fetch(url(0, "/other"))).status).toBe(200);
    for (const path of ["/other", "/healthz", "/tools/invoke", "/"]) {
      expect((await fetch(url(1, path))).status, path).toBe(404);
    }
    expect(tryBeginGatewaySuspendAdmission(() => {})?.commit()).toBe(true);
    expect(
      (
        await fetch(url(1), {
          method: "POST",
          body,
          headers: { "x-webhook-secret": "synthetic-secret" },
        })
      ).status,
    ).toBe(503);
    expect(warn).not.toHaveBeenCalled();
  });

  it.each([
    {
      name: "protected namespace",
      path: "/api/channels/telegram",
      primaryStatus: 401,
      forwardedPrimaryStatus: 403,
      primaryBody: undefined,
    },
    {
      name: "capability rewrite",
      path: "/__openclaw__/cap/vendor/webhook",
      primaryStatus: 200,
      forwardedPrimaryStatus: 200,
      primaryBody: "rewritten sibling: /webhook?oc_cap=vendor",
    },
    {
      name: "incomplete capability",
      path: "/__openclaw__/cap/vendor",
      primaryStatus: 401,
      forwardedPrimaryStatus: 401,
      primaryBody: undefined,
    },
  ])(
    "preserves the $name callback on its legacy port and the primary listener's policy",
    async ({ path, primaryStatus, forwardedPrimaryStatus, primaryBody }) => {
      expect(() =>
        register({ auth: "gateway", legacyListener: endpoint(1), throwOnFailure: true }),
      ).toThrow("legacy webhook listeners require plugin authentication");
      expect(registry.httpRoutes).toHaveLength(0);
      register({
        path,
        legacyListener: endpoint(1),
        handler: (req, res) => {
          expect(req.url).toBe(path);
          if (req.headers["x-webhook-secret"] !== "synthetic-secret") {
            res.writeHead(401).end();
            return;
          }
          expect(getPluginRuntimeGatewayRequestScope()?.client?.connect.scopes).toEqual([]);
          res.end("vendor accepted");
        },
      });
      register({
        path: "/webhook",
        legacyListener: endpoint(1),
        handler: (req, res) => {
          res.end(`rewritten sibling: ${req.url}`);
        },
      });
      const gatewayOnlyHandler = vi.fn();
      registry.httpRoutes.push({
        path,
        auth: "gateway",
        match: "exact",
        legacyListeners: [endpoint(1)],
        handler: gatewayOnlyHandler,
      });
      await listening();
      for (const forwarded of [false, true]) {
        const headers: Record<string, string> = forwarded
          ? {
              "x-forwarded-for": "203.0.113.20",
              "x-forwarded-proto": "https",
              "x-forwarded-host": "callbacks.example",
            }
          : {};
        const signed = { ...headers, "x-webhook-secret": "synthetic-secret" };
        const accepted = await fetch(url(1, path), { method: "POST", headers: signed });
        expect(accepted.status).toBe(200);
        expect(await accepted.text()).toBe("vendor accepted");
        expect(
          (
            await fetch(url(1, path), {
              method: "POST",
              headers: { ...headers, "x-webhook-secret": "wrong" },
            })
          ).status,
        ).toBe(401);
        const primary = await fetch(url(0, path), { method: "POST", headers: signed });
        expect(primary.status).toBe(forwarded ? forwardedPrimaryStatus : primaryStatus);
        const primaryText = await primary.text();
        if (forwarded && forwardedPrimaryStatus === 403) {
          expect(primaryText).toContain("proxy_attribution_required");
        }
        if (primaryBody !== undefined) {
          expect(primaryText).toBe(primaryBody);
        }
        expect(
          (await fetch(url(1, "/tools/invoke"), { method: "POST", headers: signed })).status,
        ).toBe(404);
      }
      expect(gatewayOnlyHandler).not.toHaveBeenCalled();
    },
  );

  it("retains only the restarting account's port across route handoff and registry replacement", async () => {
    let handled = 0;
    const lease = createPluginRuntimeCapabilityLease("old-account");
    const first = withPluginHttpRouteRegistry(
      registry,
      () =>
        register({
          legacyListener: endpoint(1),
          handler: (req, res) => {
            handled += 1;
            res.end(String(getWebhookLegacyListener(req)?.port));
          },
        }),
      lease,
    );
    const second = register({ legacyListener: endpoint(2) });
    await listening();
    for (const offset of [1, 2]) {
      expect(await (await fetch(url(offset))).text()).toBe(String(claim.port + offset));
    }
    const originalListeners = httpServers.slice(1);
    const handoff = createPluginHttpRouteHandoff();
    cleanups.push(handoff.release);
    handoff.park(lease);
    lease.revoke();
    const expectParkedEndpoint = async () => {
      const before = handled;
      const response = await fetch(url(1));
      expect(response.status).toBe(503);
      expect(response.headers.get("retry-after")).toBe("1");
      expect(await response.text()).toBe("plugin route is restarting; retry");
      expect(handled).toBe(before);
    };
    await expectParkedEndpoint();
    expect(await (await fetch(url(2))).text()).toBe(String(claim.port + 2));
    const revived = register({ legacyListener: endpoint(1) });
    expect(await (await fetch(url(1))).text()).toBe(String(claim.port + 1));
    revived();
    await expectParkedEndpoint();
    const oldPortClosed = once(originalListeners[1]!, "close");
    second();
    await oldPortClosed;
    expect((await fetch(url(1))).status).toBe(503);
    expect(httpServers.slice(1)).toEqual([originalListeners[0]]);
    const next = createEmptyPluginRegistry();
    adoptPluginHttpRouteHandoffs(registry, next);
    registry = next;
    const successor = register({
      legacyListener: endpoint(1),
      handler: (_req, res) => {
        register({ path: "/registered-by-request", throwOnFailure: true });
        res.end("replacement");
      },
    });
    first();
    expect(await (await fetch(url(1))).text()).toBe("replacement");
    expect(httpServers.slice(1)).toEqual([originalListeners[0]]);
    handoff.release();
    expect(httpServers.slice(1)).toEqual([originalListeners[0]]);
    expect(await (await fetch(url(1))).text()).toBe("replacement");
    const finalPortClosed = once(originalListeners[0]!, "close");
    successor();
    await finalPortClosed;
    expect(httpServers).toEqual([gatewayServer]);
  });

  it.each([true, false])(
    "settles real HTTP expectations (Gateway event handlers: %s)",
    async (gatewayEvents) => {
      const body = "synthetic webhook body";
      let handled = 0;
      register({
        legacyListener: endpoint(1),
        handler: async (req, res) => {
          expect(getWebhookLegacyListener(req)).toEqual(endpoint(1));
          expect(await readRequestBodyWithLimit(req, { maxBytes: 1024 })).toBe(body);
          handled += 1;
          res.end("accepted");
        },
      });
      await listening();
      const eventListeners = (["checkContinue", "checkExpectation"] as const).map((event) => ({
        event,
        listeners: gatewayServer.listeners(event),
      }));
      if (!gatewayEvents) {
        for (const { event } of eventListeners) {
          gatewayServer.removeAllListeners(event);
        }
      }
      const sendExpectation = (expectation: string) =>
        new Promise<{ status: number | undefined; continues: number }>((resolve, reject) => {
          let continues = 0;
          const req = request(
            url(1),
            {
              method: "POST",
              headers: {
                Expect: expectation,
                "Content-Length": Buffer.byteLength(body),
                Connection: "close",
              },
            },
            (res) => {
              res.on("error", reject);
              res.on("end", () => {
                req.destroy();
                resolve({ status: res.statusCode, continues });
              });
              res.resume();
            },
          );
          req.on("error", reject);
          req.on("continue", () => {
            continues += 1;
            req.end(body);
          });
          req.flushHeaders();
        });
      try {
        expect(await sendExpectation("100-continue")).toEqual({ status: 200, continues: 1 });
        expect(await sendExpectation("unsupported-expectation")).toEqual({
          status: 417,
          continues: 0,
        });
        expect(handled).toBe(1);
      } finally {
        if (!gatewayEvents) {
          for (const { event, listeners } of eventListeners) {
            for (const listener of listeners) {
              gatewayServer.on(event, listener);
            }
          }
        }
      }
    },
  );

  it("reports an occupied port and retries it after the conflicting route is removed", async () => {
    const removeBlocker = register({
      path: "/occupying-webhook",
      legacyListener: { port: claim.port + 1, host: "0.0.0.0" },
    });
    await listening();
    const blocker = httpServers[1]!;
    register({ legacyListener: endpoint(1) });
    await Promise.resolve();
    const listener = httpServers[2]!;
    await once(listener, "error");
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining(`Legacy webhook listener 127.0.0.1:${claim.port + 1} failed`),
    );
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("update the external callback or reverse proxy to the Gateway port"),
    );
    expect(await (await fetch(url(0))).text()).toBe("accepted");

    const closed = once(blocker, "close");
    removeBlocker();
    await closed;
    expect(await (await fetch(url(1))).text()).toBe("accepted");
  });
});
