/**
 * MuseProvider — provider snapshot, health check, auth detection, and model
 * discovery for the Muse Code CLI over persistent MSP.
 *
 * @module provider/Layers/MuseProvider
 */
import * as NodeOS from "node:os";
import type {
  ModelCapabilities,
  MuseSettings,
  ServerProvider,
  ServerProviderAuth,
  ServerProviderModel,
  ServerProviderState,
} from "@t3tools/contracts";
import { MUSE_DEFAULT_MODEL } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { HttpClient } from "effect/unstable/http";

import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
  type ServerProviderPresentation,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import { makeMspHost } from "../msp/MspClient.ts";

export const MUSE_PRESENTATION: ServerProviderPresentation = {
  displayName: "Muse",
  showInteractionModeToggle: false,
  reportsContextWindow: true,
};

export const MUSE_REASONING_EFFORTS = [
  { value: "none", label: "None" },
  { value: "minimal", label: "Minimal" },
  { value: "low", label: "Low" },
  { value: "medium", label: "Medium" },
  { value: "high", label: "High" },
  { value: "xhigh", label: "Extra High" },
  { value: "max", label: "Max" },
  { value: "ultra", label: "Ultra" },
] as const;

export const DEFAULT_MUSE_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    buildSelectOptionDescriptor({
      id: "reasoningEffort",
      label: "Reasoning Effort",
      options: MUSE_REASONING_EFFORTS,
    }),
  ],
});

export function getMuseFallbackModels(settings: MuseSettings): ReadonlyArray<ServerProviderModel> {
  const builtIn: ServerProviderModel[] = [
    {
      slug: MUSE_DEFAULT_MODEL,
      name: "Muse Spark 1.3",
      isCustom: false,
      isDefault: true,
      capabilities: DEFAULT_MUSE_MODEL_CAPABILITIES,
    },
    {
      slug: "muse-spark-1.3-contributor",
      name: "Muse Spark 1.3 Contributor",
      isCustom: false,
      capabilities: DEFAULT_MUSE_MODEL_CAPABILITIES,
    },
    {
      slug: "muse-spark-1.2",
      name: "Muse Spark 1.2",
      isCustom: false,
      capabilities: DEFAULT_MUSE_MODEL_CAPABILITIES,
    },
    {
      slug: "muse-spark-1.2-contributor",
      name: "Muse Spark 1.2 Contributor",
      isCustom: false,
      capabilities: DEFAULT_MUSE_MODEL_CAPABILITIES,
    },
    {
      slug: "echo",
      name: "Echo (Offline Test)",
      isCustom: false,
      capabilities: createModelCapabilities({ optionDescriptors: [] }),
    },
  ];

  return providerModelsFromSettings(
    builtIn,
    settings.customModels,
    DEFAULT_MUSE_MODEL_CAPABILITIES,
  );
}

export function buildInitialMuseProviderSnapshot(
  settings: MuseSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = getMuseFallbackModels(settings);

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: MUSE_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        slashCommands: [COMPACT_SLASH_COMMAND],
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Muse is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: MUSE_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      slashCommands: [COMPACT_SLASH_COMMAND],
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Muse availability...",
      },
    });
  });
}

interface MuseAuthMetadata {
  readonly auth: ServerProviderAuth;
}

const MetaProviderAuthJsonSchema = Schema.Struct({
  providers: Schema.optional(
    Schema.Struct({
      meta: Schema.optional(
        Schema.Struct({
          user_email: Schema.optional(Schema.NullOr(Schema.String)),
          user_full_name: Schema.optional(Schema.NullOr(Schema.String)),
          mechanism: Schema.optional(Schema.NullOr(Schema.String)),
        }),
      ),
    }),
  ),
});

const decodeAuthJson = Schema.decodeUnknownOption(
  Schema.fromJsonString(MetaProviderAuthJsonSchema),
);

function probeMuseAuth(processEnv: NodeJS.ProcessEnv = process.env) {
  return Effect.gen(function* () {
    // 1. Explicit API key in environment
    const apiKey = processEnv.META_API_KEY;
    if (apiKey && apiKey.trim().length > 0) {
      return {
        auth: {
          status: "authenticated",
          type: "api-key",
          label: "Meta API Key",
        },
      } satisfies MuseAuthMetadata;
    }

    // 2. Credentials in ~/.config/muse/auth.json
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const home = processEnv.HOME ?? NodeOS.homedir();
    const authPath = path.join(home, ".config", "muse", "auth.json");

    const exists = yield* fs.exists(authPath).pipe(Effect.orElseSucceed(() => false));
    if (exists) {
      const content = yield* fs.readFileString(authPath).pipe(Effect.orElseSucceed(() => ""));
      if (content) {
        const parsedOption = decodeAuthJson(content);
        if (Option.isSome(parsedOption)) {
          const meta = parsedOption.value.providers?.meta;
          if (meta) {
            const userEmail = meta.user_email?.trim();
            const label = userEmail ? `Meta account (${userEmail})` : "Meta account";
            return {
              auth: {
                status: "authenticated",
                type: meta.mechanism ?? "oauth",
                label,
              },
            } satisfies MuseAuthMetadata;
          }
        }
      }
    }

    return {
      auth: {
        status: "unauthenticated",
        label: "Meta account",
      },
    } satisfies MuseAuthMetadata;
  });
}

export function makeMuseModelDiscovery(
  settings: MuseSettings,
  processEnv: NodeJS.ProcessEnv = process.env,
) {
  return Effect.succeed(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const hostResult = yield* Effect.scoped(
        Effect.gen(function* () {
          const host = yield* makeMspHost({
            command: settings.binaryPath || "muse",
            env: processEnv,
          });
          return yield* host.listModels();
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.catch(() => Effect.succeed(null)),
        ),
      );

      if (!hostResult || hostResult.length === 0) {
        return getMuseFallbackModels(settings);
      }

      const seen = new Set<string>();
      const discovered: ServerProviderModel[] = [];

      for (const entry of hostResult) {
        const slug = entry.modelId.trim();
        if (!slug || seen.has(slug)) continue;
        seen.add(slug);

        discovered.push({
          slug,
          name: entry.displayLabel?.trim() || slug,
          isCustom: false,
          isDefault: entry.isDefault || slug === MUSE_DEFAULT_MODEL,
          capabilities: DEFAULT_MUSE_MODEL_CAPABILITIES,
        });
      }

      // Always ensure offline echo model is present for smoke tests / testing
      if (!seen.has("echo")) {
        discovered.push({
          slug: "echo",
          name: "Echo (Offline Test)",
          isCustom: false,
          capabilities: createModelCapabilities({ optionDescriptors: [] }),
        });
      }

      return providerModelsFromSettings(
        discovered,
        settings.customModels,
        DEFAULT_MUSE_MODEL_CAPABILITIES,
      );
    }),
  );
}

export function checkMuseProviderStatus(
  settings: MuseSettings,
  processEnv: NodeJS.ProcessEnv = process.env,
  discoverModels?: Effect.Effect<
    ReadonlyArray<ServerProviderModel>,
    never,
    ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem
  >,
) {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);

    if (!settings.enabled) {
      return yield* buildInitialMuseProviderSnapshot(settings);
    }

    const binary = settings.binaryPath || "muse";
    const spawnCommand = yield* resolveSpawnCommand(binary, ["--version"], {
      env: processEnv,
    });
    const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: processEnv,
    });

    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const versionResult = yield* spawnAndCollect(binary, command).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.catch((err) =>
        isCommandMissingCause(err)
          ? Effect.succeed({ stdout: "", stderr: "", code: 127 })
          : Effect.succeed({ stdout: "", stderr: "", code: 1 }),
      ),
    );

    if (versionResult.code === 127 || (!versionResult.stdout && versionResult.code !== 0)) {
      return buildServerProvider({
        presentation: MUSE_PRESENTATION,
        enabled: true,
        checkedAt,
        models: getMuseFallbackModels(settings),
        slashCommands: [COMPACT_SLASH_COMMAND],
        probe: {
          installed: false,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: `Muse CLI command \`${binary}\` was not found. Install Muse Code CLI, ensure it is on PATH, then restart T3 Code.`,
        },
      });
    }

    const version = parseGenericCliVersion(versionResult.stdout);
    const { auth } = yield* probeMuseAuth(processEnv);

    const models = discoverModels
      ? yield* discoverModels.pipe(
          Effect.catch(() => Effect.succeed(getMuseFallbackModels(settings))),
        )
      : getMuseFallbackModels(settings);

    const isAuthReady = auth.status === "authenticated";
    const status: Exclude<ServerProviderState, "disabled"> = isAuthReady ? "ready" : "warning";
    const message = isAuthReady
      ? undefined
      : "Muse is installed. Sign in to your Meta account with `muse login` or provide META_API_KEY.";

    return {
      ...buildServerProvider({
        presentation: MUSE_PRESENTATION,
        enabled: true,
        checkedAt,
        models,
        slashCommands: [COMPACT_SLASH_COMMAND],
        probe: {
          installed: true,
          version,
          status,
          auth,
          ...(message ? { message } : {}),
        },
      }),
      supportsConversationRollback: false,
      supportsTextGeneration: true,
    } satisfies ServerProviderDraft;
  });
}

export function enrichMuseSnapshot(input: {
  readonly settings: MuseSettings;
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities?: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly stampIdentity?: (snapshot: ServerProvider) => ServerProvider;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> {
  const { settings, snapshot, publishSnapshot } = input;
  const stampIdentity = input.stampIdentity ?? ((value) => value);

  if (
    !settings.enabled ||
    snapshot.auth.status === "unauthenticated" ||
    !input.maintenanceCapabilities
  ) {
    return Effect.void;
  }

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) =>
      publishSnapshot(stampIdentity(enrichedSnapshot)).pipe(Effect.as(enrichedSnapshot)),
    ),
    Effect.catch(() => Effect.void),
  );
}
