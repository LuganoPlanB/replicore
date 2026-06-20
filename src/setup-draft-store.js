import { chmod, mkdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"

import { base58Encode } from "./base58.js"
import { normalizeSetupDraft } from "./setup-validation.js"

export const SETUP_DRAFT_SCHEMA_VERSION = 1

/**
 * Persist one setup draft as formatted JSON.
 *
 * @param {string} filePath
 * @param {unknown} draft
 */
export async function writeSetupDraft(filePath, draft) {
  const absolutePath = path.resolve(filePath)
  const normalized = normalizeSetupDraft({
    ...draft,
    schemaVersion: draft?.schemaVersion ?? SETUP_DRAFT_SCHEMA_VERSION
  })

  await mkdir(path.dirname(absolutePath), { recursive: true })
  await writeFile(absolutePath, `${JSON.stringify(normalized, null, 2)}\n`, {
    mode: 0o600
  })

  try {
    await chmod(absolutePath, 0o600)
  } catch {
    // Windows and some filesystems do not honor POSIX permissions.
  }

  return {
    path: absolutePath,
    draft: normalized
  }
}

/**
 * Load one persisted setup draft from JSON.
 *
 * @param {string} filePath
 */
export async function readSetupDraft(filePath) {
  const absolutePath = path.resolve(filePath)
  let raw
  try {
    raw = JSON.parse(await readFile(absolutePath, "utf8"))
  } catch (error) {
    if (error?.code === "ENOENT") throw error
    return null
  }

  if (/^[0-9a-f]{64}$/.test(raw.clusterSecret ?? "")) {
    raw.clusterSecret = base58Encode(Buffer.from(raw.clusterSecret, "hex"))
  }
  if (/^[0-9a-f]{64}$/.test(raw.machineId ?? "")) {
    raw.machineId = base58Encode(Buffer.from(raw.machineId, "hex"))
  }

  try {
    return normalizeSetupDraft(raw)
  } catch {
    return null
  }
}
