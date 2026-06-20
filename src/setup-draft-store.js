import { chmod, mkdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"

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
  return normalizeSetupDraft(JSON.parse(await readFile(absolutePath, "utf8")))
}
