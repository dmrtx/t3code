// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpClient } from "effect/unstable/http";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { MuseDriver } from "./MuseDriver.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-muse-driver-test-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Disabled Muse must not make an HTTP request")),
    ),
  ),
);

it.layer(testLayer)("MuseDriver", (it) => {
  it.effect("instantiates a disabled Muse driver instance", () =>
    Effect.gen(function* () {
      expect(MuseDriver.driverKind).toBe(ProviderDriverKind.make("muse"));
      expect(MuseDriver.metadata.displayName).toBe("Muse");

      const defaultConfig = MuseDriver.defaultConfig();
      expect(defaultConfig.enabled).toBe(false);

      const instance = yield* MuseDriver.create({
        instanceId: ProviderInstanceId.make("muse-test"),
        displayName: "Muse Test",
        enabled: false,
        environment: [],
        config: { ...defaultConfig, enabled: false },
      });

      expect(instance.adapter.provider).toBe(ProviderDriverKind.make("muse"));
      expect(instance.textGeneration).toBeDefined();

      const refreshSnapshot = yield* instance.snapshot.refresh;
      expect(refreshSnapshot.status).toBe("disabled");

      const maintenance = yield* instance.snapshot.resolveMaintenance();
      expect(maintenance.provider).toBe(ProviderDriverKind.make("muse"));
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Disabled Muse must not spawn a process")),
      ),
      Effect.scoped,
    ),
  );
});
