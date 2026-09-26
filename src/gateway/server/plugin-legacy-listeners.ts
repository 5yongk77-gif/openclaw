import { AsyncLocalStorage } from "node:async_hooks";
import { createServer, type Server } from "node:http";
import { runHttpConnectionRequest } from "../../infra/http-request-lifecycle.js";
import { markPluginHttpLegacyListener } from "../../plugins/http-legacy-listener.js";
import { onPluginHttpRoutesChanged } from "../../plugins/http-route-owner.js";
import type { PluginHttpRouteRegistration, PluginRegistry } from "../../plugins/registry-types.js";

type LegacyEndpoint = NonNullable<PluginHttpRouteRegistration["legacyListeners"]>[number];
type LegacyListener = {
  server: Server;
  controller: AbortController;
  endpoint: LegacyEndpoint;
  defaultTimeouts: NonNullable<LegacyEndpoint["timeouts"]>;
};
const endpointKey = ({ host, port }: LegacyEndpoint) => `${host ?? "<unspecified>"}:${port}`;

/** Compatibility ports share Gateway dispatch and the route owner's existing handoff leases. */
export function startPluginLegacyListeners(params: {
  gatewayServer: Server;
  httpServers: Server[];
  getRegistry: () => PluginRegistry;
  warn: (message: string) => void;
}): () => void {
  const listeners = new Map<string, LegacyListener>();
  // Channel publications cannot lend their account lifetime to a shared listener.
  const runInGatewayContext = AsyncLocalStorage.snapshot();
  let stopped = false;
  let queued = false;
  const close = ({ server, controller }: LegacyListener) => {
    controller.abort();
    server.close();
    server.closeAllConnections();
    const index = params.httpServers.indexOf(server);
    if (index !== -1) {
      params.httpServers.splice(index, 1);
    }
  };
  const reconcile = () => {
    queued = false;
    if (stopped) {
      return;
    }
    const endpoints = new Map(
      params
        .getRegistry()
        .httpRoutes.flatMap((route) =>
          (route.legacyListeners ?? []).map(
            (endpoint) => [endpointKey(endpoint), endpoint] as const,
          ),
        ),
    );
    for (const [key, listener] of listeners) {
      if (!endpoints.has(key)) {
        listeners.delete(key);
        close(listener);
      }
    }
    for (const [key, endpoint] of endpoints) {
      const existing = listeners.get(key);
      if (existing) {
        existing.endpoint = endpoint;
        const timeouts = endpoint.timeouts ?? existing.defaultTimeouts;
        existing.server.headersTimeout = timeouts.headers;
        existing.server.requestTimeout = timeouts.request;
        existing.server.setTimeout(timeouts.socket);
        continue;
      }
      const server = createServer();
      const controller = new AbortController();
      const listener: LegacyListener = {
        server,
        controller,
        endpoint,
        defaultTimeouts: {
          headers: server.headersTimeout,
          request: server.requestTimeout,
          socket: server.timeout,
        },
      };
      if (endpoint.timeouts) {
        server.headersTimeout = endpoint.timeouts.headers;
        server.requestTimeout = endpoint.timeouts.request;
        server.setTimeout(endpoint.timeouts.socket);
      }
      // Native Node expectations and Upgrade fallback match the shipped private servers.
      server.on("request", (req, res) => {
        const endpoint = listener.endpoint;
        if (endpoint.health && req.url === endpoint.health.path) {
          void runHttpConnectionRequest(
            req,
            async () => {
              if (endpoint.health?.contentType) {
                res.setHeader("Content-Type", endpoint.health.contentType);
              }
              res.writeHead(200);
              res.end("ok");
            },
            res,
          ).catch((error: unknown) => res.destroy(error instanceof Error ? error : undefined));
          return;
        }
        markPluginHttpLegacyListener(req, endpoint);
        params.gatewayServer.emit("request", req, res);
      });
      server.on("error", (error) => {
        const listener = listeners.get(key);
        if (listener?.server === server) {
          listeners.delete(key);
          close(listener);
        }
        params.warn(
          `Legacy webhook listener ${key} failed: ${String(error)}. ` +
            "The Gateway webhook route remains available; update the external callback or reverse proxy to the Gateway port.",
        );
      });
      if (endpoint.port === 0) {
        server.once("listening", () => {
          const address = server.address();
          if (address && typeof address !== "string") {
            params.warn(
              `Legacy webhook port 0 selected ${address.address}:${address.port}; this port changes on restart. ` +
                "Update the external callback or reverse proxy to the Gateway port.",
            );
          }
        });
      }
      listeners.set(key, listener);
      params.httpServers.push(server);
      try {
        server.listen({
          port: endpoint.port,
          host: endpoint.host,
          signal: controller.signal,
        });
      } catch (error) {
        server.emit("error", error);
      }
    }
  };
  const stopWatching = onPluginHttpRoutesChanged(() => {
    if (!queued && !stopped) {
      queued = true;
      queueMicrotask(() => runInGatewayContext(reconcile));
    }
  });
  const stop = () => {
    if (stopped) {
      return;
    }
    stopped = true;
    stopWatching();
    params.gatewayServer.off("close", stop);
    for (const listener of listeners.values()) {
      close(listener);
    }
    listeners.clear();
  };
  params.gatewayServer.once("close", stop);
  reconcile();
  return stop;
}
