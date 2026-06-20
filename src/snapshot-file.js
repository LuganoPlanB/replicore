import { createHash } from "node:crypto"
import { mkdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"

import { canonicalize } from "./canonical.js"

export async function writeSnapshotFile(filePath, snapshot) {
  const absolutePath = path.resolve(filePath)
  await mkdir(path.dirname(absolutePath), { recursive: true })
  await writeFile(absolutePath, JSON.stringify(snapshot, null, 2))
  return absolutePath
}

export async function readSnapshotFile(filePath) {
  const absolutePath = path.resolve(filePath)
  return JSON.parse(await readFile(absolutePath, "utf8"))
}

export function normalizeSnapshotEnvelope(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return null
  if (snapshot.version !== 2) return null
  if (!snapshot.content || typeof snapshot.content !== "object") return null
  if (typeof snapshot.contentHash !== "string" || snapshot.contentHash.length === 0) return null
  return snapshot
}

export function snapshotContentHash(snapshot) {
  return createHash("sha256").update(canonicalize(snapshot)).digest("hex")
}

/**
 * Build a versioned snapshot envelope from the node's in-memory state.
 *
 * @param {{
 *   view: import("./materialized-view.js").MaterializedView,
 *   usesSharedAuthoritativeLog: () => boolean,
 *   getAuthoritativeLogStatus: () => Promise<{ lastLogIndex: number, lastLogTerm: number }>,
 *   currentLeader: () => string | null,
 *   lastKnownLeaderId: () => string | null,
 *   membershipVersion: number,
 *   lastAppliedIndex: number
 * }} ctx
 */
export async function createSnapshot(ctx) {
  const content = await ctx.view.exportSnapshot()
  if (!ctx.usesSharedAuthoritativeLog()) {
    return content
  }

  const authoritativeLog = await ctx.getAuthoritativeLogStatus()
  const snapshot = {
    version: 2,
    createdAt: new Date().toISOString(),
    leaderNodeId: ctx.currentLeader() ?? ctx.lastKnownLeaderId(),
    membershipVersion: ctx.membershipVersion,
    lastIncludedIndex: ctx.lastAppliedIndex,
    lastIncludedTerm: authoritativeLog.lastLogTerm,
    content
  }
  return {
    ...snapshot,
    contentHash: snapshotContentHash(snapshot)
  }
}

/**
 * Restore a snapshot envelope into the in-memory view, verifying integrity first.
 *
 * Returns the parsed envelope so the caller can update consensus state.
 *
 * @param {{
 *   view: import("./materialized-view.js").MaterializedView,
 *   snapshot: unknown,
 *   currentLeader: () => string | null,
 *   onPostRestore?: (envelope: { leaderNodeId: string | null, lastIncludedIndex?: number }) => Promise<void>
 * }} ctx
 */
export async function restoreSnapshot(ctx) {
  const envelope = normalizeSnapshotEnvelope(ctx.snapshot)
  if (envelope) {
    const expectedHash = snapshotContentHash({
      version: envelope.version,
      createdAt: envelope.createdAt,
      leaderNodeId: envelope.leaderNodeId,
      membershipVersion: envelope.membershipVersion,
      lastIncludedIndex: envelope.lastIncludedIndex,
      lastIncludedTerm: envelope.lastIncludedTerm,
      content: envelope.content
    })
    if (expectedHash !== envelope.contentHash) {
      throw new Error("Snapshot content hash mismatch")
    }
    if (ctx.currentLeader() && envelope.leaderNodeId && envelope.leaderNodeId !== ctx.currentLeader()) {
      throw new Error("Snapshot leader identity does not match current leader")
    }
    await ctx.view.importSnapshot(envelope.content)
    if (ctx.onPostRestore) {
      await ctx.onPostRestore(envelope)
    }
    return
  }

  await ctx.view.importSnapshot(ctx.snapshot)
}
