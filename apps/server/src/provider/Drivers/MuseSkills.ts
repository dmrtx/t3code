/**
 * MuseSkills — discovery of Muse skills for the `$` picker and context injection.
 *
 * Uses `muse skills list --json` (scoped to workspace when available) to discover
 * active skills, mapping them to canonical ServerProviderSkill entries.
 *
 * @module provider/Drivers/MuseSkills
 */
import type { MuseSettings, ServerProviderSkill } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { spawnAndCollect } from "../providerSnapshot.ts";

const MuseSkillItemSchema = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  display_name: Schema.optional(Schema.NullOr(Schema.String)),
  description: Schema.optional(Schema.NullOr(Schema.String)),
  path: Schema.String,
  scope: Schema.optional(Schema.NullOr(Schema.String)),
  activation: Schema.optional(Schema.NullOr(Schema.String)),
});

const MuseSkillsListOutputSchema = Schema.Struct({
  skills: Schema.Array(MuseSkillItemSchema),
});

const decodeSkillsOutput = Schema.decodeUnknownEffect(
  Schema.fromJsonString(MuseSkillsListOutputSchema),
);

function normalizeScope(scope: string | null | undefined): "user" | "project" | "builtin" {
  const s = scope?.toLowerCase().trim();
  if (s === "project" || s === "workspace") return "project";
  if (s === "built-in" || s === "builtin") return "builtin";
  return "user";
}

function expandHome(filepath: string, env: NodeJS.ProcessEnv): string {
  const home = env.HOME ?? env.USERPROFILE ?? "";
  if (filepath.startsWith("$HOME")) {
    return home + filepath.slice("$HOME".length);
  }
  if (filepath.startsWith("~")) {
    return home + filepath.slice(1);
  }
  return filepath;
}

export function probeMuseSkills(
  cwd: string,
  settings: MuseSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.Effect<
  ReadonlyArray<ServerProviderSkill>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Path.Path
> {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const binary = settings.binaryPath || "muse";
    const args = ["skills", "list", "--enabled-only", "--workspace", cwd, "--json"];
    const spawnCommand = yield* resolveSpawnCommand(binary, args, { env: environment });

    const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      cwd,
      env: environment,
    });

    const result = yield* spawnAndCollect(binary, command).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.catch(() => Effect.succeed({ stdout: "", stderr: "", code: 1 })),
    );

    if (result.code !== 0 || !result.stdout.trim()) {
      return [] as ReadonlyArray<ServerProviderSkill>;
    }

    const parsed = yield* decodeSkillsOutput(result.stdout).pipe(
      Effect.catch(() => Effect.succeed({ skills: [] })),
    );

    const seen = new Set<string>();
    const skills: ServerProviderSkill[] = [];

    for (const item of parsed.skills) {
      const name = item.name.trim() || item.id.trim();
      if (!name || seen.has(name)) continue;
      seen.add(name);

      const path = expandHome(item.path.trim(), environment);
      const description = item.description?.trim() || undefined;
      const scope = normalizeScope(item.scope);

      skills.push({
        name,
        ...(description ? { description } : {}),
        path,
        scope,
        enabled: true,
      });
    }

    return skills;
  });
}
