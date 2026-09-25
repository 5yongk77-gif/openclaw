import { AsyncLocalStorage } from "node:async_hooks";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { runHttpConnectionRequest } from "../../infra/http-request-lifecycle.js";
import { markPluginHttpLegacyListener } from "../../plugins/http-legacy-listener.js";
import { onPluginHttpRoutesChanged } from "../../plugins/http-route-owner.js";
import type { PluginHttpRouteRegistration, PluginRegistry } from "../../plugins/registry-types.js";

type LegacyEndpoint = NonNullable<PluginHttpRouteRegistration["legacyListeners"]>[number];
type LegacyListener = { server: Server; controller: AbortController };
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
      if (listeners.has(key)) {
        continue;
      }
      const server = createServer({
        maxHeaderSize: 16 * 1024,
        headersTimeout: 10_000,
        requestTimeout: 30_000,
        keepAliveTimeout: 5_000,
      });
      const controller = new AbortController();
      server.setTimeout(30_000, (socket) => socket.destroy());
      const forward =
        (event: "request" | "checkContinue" | "checkExpectation") =>
        (req: IncomingMessage, res: ServerResponse) => {
          markPluginHttpLegacyListener(req, endpoint);
          // Even a path rejection must wait for earlier responses on this connection.
          if (!params.gatewayServer.emit(event, req, res)) {
            if (event === "checkContinue") {
              res.writeContinue();
              params.gatewayServer.emit("request", req, res);
            } else if (event === "checkExpectation") {
              res.writeHead(417).end();
            }
          }
        };
      server.on("request", forward("request"));
      server.on("checkContinue", forward("checkContinue"));
      server.on("checkExpectation", forward("checkExpectation"));
      for (const event of ["upgrade", "connect"] as const) {
        server.on(event, (req, socket) => {
          void runHttpConnectionRequest(
            req,
            async () => {
              socket.destroy();
            },
            "upgrade",
          );
        });
      }
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
      listeners.set(key, { server, controller });
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
