import { describe, expect, it } from "@effect/vitest";
import { NodeServices } from "@effect/platform-node";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import { devinUserStatusToLimits, readDevinUsageLimits } from "./devinUsageLimits.ts";

describe("devinUserStatusToLimits", () => {
  const checkedAt = "2026-10-29T14:00:00.000Z";

  it("maps Devin's remaining quota to used daily and weekly windows", () => {
    expect(
      devinUserStatusToLimits(
        {
          userStatus: {
            planStatus: {
              dailyQuotaRemainingPercent: 22,
              dailyQuotaResetAtUnix: "1793347200",
              // proto3 JSON omits a zero remaining percent.
              weeklyQuotaResetAtUnix: "1793520000",
            },
          },
        },
        checkedAt,
      )?.windows,
    ).toEqual([
      {
        id: "weekly",
        kind: "weekly",
        label: "Weekly",
        usedPercent: 100,
        windowDurationMins: 10080,
        resetsAt: "2026-11-01T08:00:00.000Z",
      },
      {
        id: "daily",
        kind: "other",
        label: "Daily",
        usedPercent: 78,
        windowDurationMins: 1440,
        resetsAt: "2026-10-30T08:00:00.000Z",
      },
    ]);
  });

  it("keeps the dashboard fallback when the plan has no quota windows", () => {
    expect(devinUserStatusToLimits({}, checkedAt)).toBeUndefined();
    expect(
      devinUserStatusToLimits(
        { userStatus: { planStatus: { dailyQuotaRemainingPercent: 50 } } },
        checkedAt,
      ),
    ).toBeUndefined();
  });

  it.effect("sends the CLI login and version to the CLI's own server", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dataHome = yield* fs.makeTempDirectoryScoped();
      yield* fs.makeDirectory(path.join(dataHome, "devin"));
      yield* fs.writeFileString(
        path.join(dataHome, "devin", "credentials.toml"),
        'api_server_url = "https://devin.example"\nwindsurf_api_key = "cli-key"\n',
      );
      const client = HttpClient.make((request) => {
        expect(request.url).toBe(
          "https://devin.example/exa.seat_management_pb.SeatManagementService/GetUserStatus",
        );
        expect(
          request.body._tag === "Uint8Array"
            ? JSON.parse(new TextDecoder().decode(request.body.body))
            : null,
        ).toEqual({
          metadata: {
            apiKey: "cli-key",
            ideName: "devin-cli",
            ideVersion: "3000.11.228",
            extensionName: "devin-cli",
            extensionVersion: "3000.11.228",
          },
        });
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            Response.json({
              userStatus: {
                planStatus: {
                  weeklyQuotaRemainingPercent: 40,
                  weeklyQuotaResetAtUnix: "1793520000",
                },
              },
            }),
          ),
        );
      });
      const read = (version: string) =>
        readDevinUsageLimits({ XDG_DATA_HOME: dataHome }, version).pipe(
          Effect.provideService(HostProcess.Platform, "darwin"),
          Effect.provideService(HttpClient.HttpClient, client),
        );
      expect((yield* read("3000.11.228"))?.windows.map((window) => window.usedPercent)).toEqual([
        60,
      ]);
      // A custom executable has no known version, and the server rejects requests without one.
      expect(yield* read("")).toBeUndefined();
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
