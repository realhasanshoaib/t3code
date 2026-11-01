import type { ServerProviderUsageLimits, ServerProviderUsageWindow } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import { clampPercent, makeUsageLimits } from "@t3tools/provider-core/server/usageLimits";

// The request carries the CLI login, so it must never follow a redirect off the CLI's server.
const NO_REDIRECT: RequestInit = { redirect: "error" };
const UnixSeconds = Schema.Union([Schema.String, Schema.Number]);
const DevinUserStatus = Schema.Struct({
  userStatus: Schema.optional(
    Schema.Struct({
      planStatus: Schema.optional(
        Schema.Struct({
          dailyQuotaRemainingPercent: Schema.optional(Schema.Number),
          weeklyQuotaRemainingPercent: Schema.optional(Schema.Number),
          dailyQuotaResetAtUnix: Schema.optional(UnixSeconds),
          weeklyQuotaResetAtUnix: Schema.optional(UnixSeconds),
        }),
      ),
    }),
  ),
});

export function devinUserStatusToLimits(
  response: typeof DevinUserStatus.Type,
  checkedAt: string,
): ServerProviderUsageLimits | undefined {
  const plan = response.userStatus?.planStatus;
  if (!plan) return undefined;
  const windows: ServerProviderUsageWindow[] = [];
  for (const [id, kind, label, minutes, remaining, resetAt] of [
    ["daily", "other", "Daily", 1440, plan.dailyQuotaRemainingPercent, plan.dailyQuotaResetAtUnix],
    [
      "weekly",
      "weekly",
      "Weekly",
      10080,
      plan.weeklyQuotaRemainingPercent,
      plan.weeklyQuotaResetAtUnix,
    ],
  ] as const) {
    const reset = DateTime.make(Number(resetAt) * 1000);
    // Plans without a quota window carry no reset time.
    if (!(Number(resetAt) > 0) || Option.isNone(reset)) continue;
    windows.push({
      id,
      kind,
      label,
      // proto3 JSON omits zero, so a missing remaining percent means the window is used up.
      usedPercent: clampPercent(100 - (remaining ?? 0)),
      windowDurationMins: minutes,
      resetsAt: DateTime.formatIso(reset.value),
    });
  }
  return windows.length > 0 ? makeUsageLimits({ checkedAt, windows }) : undefined;
}

const tomlString = (source: string, key: string) =>
  source.match(new RegExp(`^\\s*${key}\\s*=\\s*"([^"\\\\]+)"`, "m"))?.[1];

export const readDevinUsageLimits = Effect.fn("readDevinUsageLimits")(function* (
  environment: NodeJS.ProcessEnv,
  cliVersion: string,
) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  return yield* Effect.gen(function* () {
    if (!cliVersion || (yield* HostProcess.Platform) === "win32") return undefined;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dataHome =
      environment.XDG_DATA_HOME?.trim() ||
      path.join(environment.HOME || (yield* HostProcess.HomeDirectory), ".local", "share");
    const credentials = yield* fs.readFileString(path.join(dataHome, "devin", "credentials.toml"));
    const apiKey = tomlString(credentials, "windsurf_api_key");
    const server = tomlString(credentials, "api_server_url") ?? "https://server.codeium.com";
    if (!apiKey || !server.startsWith("https://")) return undefined;
    const response = yield* (yield* HttpClient.HttpClient)
      .execute(
        HttpClientRequest.post(
          `${server.replace(/\/$/, "")}/exa.seat_management_pb.SeatManagementService/GetUserStatus`,
        ).pipe(
          HttpClientRequest.setHeaders({ "connect-protocol-version": "1" }),
          HttpClientRequest.bodyJsonUnsafe({
            metadata: {
              apiKey,
              ideName: "devin-cli",
              ideVersion: cliVersion,
              extensionName: "devin-cli",
              extensionVersion: cliVersion,
            },
          }),
        ),
      )
      .pipe(Effect.provideService(FetchHttpClient.RequestInit, NO_REDIRECT));
    return devinUserStatusToLimits(
      yield* HttpClientResponse.schemaBodyJson(DevinUserStatus)(
        yield* HttpClientResponse.filterStatusOk(response),
      ),
      checkedAt,
    );
  }).pipe(
    Effect.timeout("10 seconds"),
    Effect.orElseSucceed(() => undefined),
  );
});
