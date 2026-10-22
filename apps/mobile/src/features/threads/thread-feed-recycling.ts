import { resolveUserMessagePresentation } from "@t3tools/client-runtime/user-message";
import type { ThreadFeedEntry } from "../../lib/threadActivity";

export function threadFeedItemType(entry: ThreadFeedEntry): string {
  if (entry.type !== "message") return entry.type;
  if (resolveUserMessagePresentation(entry.message).attribution === "automation") {
    return "message:automation";
  }
  return `message:${entry.message.role}`;
}

export function threadFeedRenderKey(entry: ThreadFeedEntry): string {
  // Only assistant bodies are recycling-aware. Keep local state and nested
  // scroll restoration isolated for every other kind of row.
  return entry.type === "message" && entry.message.role === "assistant"
    ? "message:assistant"
    : entry.id;
}
