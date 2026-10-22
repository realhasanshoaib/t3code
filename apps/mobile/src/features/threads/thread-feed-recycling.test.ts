import { describe, expect, it } from "vite-plus/test";
import { MessageId, ScheduledTaskId, ThreadId } from "@t3tools/contracts";
import type { ThreadFeedEntry } from "../../lib/threadActivity";
import { threadFeedItemType, threadFeedRenderKey } from "./thread-feed-recycling";

function message(id: string, role: "user" | "assistant"): ThreadFeedEntry {
  return {
    type: "message",
    id,
    createdAt: "2026-10-22T10:00:00.000Z",
    message: {
      id: MessageId.make(id),
      role,
      text: id,
      attachments: [],
      runId: null,
      streaming: false,
      visibility: "local",
      sourceThreadId: ThreadId.make("thread"),
      createdAt: "2026-10-22T10:00:00.000Z",
      updatedAt: "2026-10-22T10:00:00.000Z",
    },
  };
}

describe("feed recycling", () => {
  it("reuses assistant bodies without sharing user-message state", () => {
    const first = message("first", "assistant");
    const second = message("second", "assistant");
    expect(threadFeedItemType(first)).toBe("message:assistant");
    expect(threadFeedItemType(second)).toBe(threadFeedItemType(first));
    expect(threadFeedRenderKey(second)).toBe(threadFeedRenderKey(first));
    expect(threadFeedRenderKey(message("user-1", "user"))).not.toBe(
      threadFeedRenderKey(message("user-2", "user")),
    );
    expect(threadFeedItemType(message("user", "user"))).toBe("message:user");
  });

  it("preserves delivery identity and separates automation from user bubbles", () => {
    const entry = message("scheduled", "user");
    if (entry.type !== "message") throw new Error("Expected a message");
    const automated = {
      ...entry,
      message: { ...entry.message, scheduledTaskId: ScheduledTaskId.make("task") },
    };
    expect(threadFeedItemType(automated)).toBe("message:automation");
    expect(threadFeedRenderKey(automated)).toBe(entry.id);
    expect(threadFeedRenderKey({ ...entry, message: { ...entry.message, streaming: true } })).toBe(
      entry.id,
    );
  });

  it("isolates work-group restoration and measured row kinds", () => {
    const group = (id: string): ThreadFeedEntry => ({
      type: "activity-group",
      id,
      createdAt: "2026-10-22T10:00:00.000Z",
      runId: null,
      activities: [],
    });
    const thinking = {
      type: "thinking",
      id: "thinking",
      createdAt: "2026-10-22T10:00:00.000Z",
      runId: null,
    } as const;
    expect(threadFeedItemType(group("tools-1"))).toBe("activity-group");
    expect(threadFeedItemType(thinking)).toBe("thinking");
    expect(threadFeedRenderKey(group("tools-1"))).not.toBe(threadFeedRenderKey(group("tools-2")));
  });
});
