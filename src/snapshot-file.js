import { mkdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"

/**
 * Save a snapshot object as formatted JSON.
 *
 * @param {string} filePath
 * @param {unknown} snapshot
 */
export async function writeSnapshotFile(filePath, snapshot) {
  const absolutePath = path.resolve(filePath)
  await mkdir(path.dirname(absolutePath), { recursive: true })
  await writeFile(absolutePath, JSON.stringify(snapshot, null, 2))
  return absolutePath
}

/**
 * Load a snapshot object from JSON.
 *
 * @param {string} filePath
 */
export async function readSnapshotFile(filePath) {
  const absolutePath = path.resolve(filePath)
  return JSON.parse(await readFile(absolutePath, "utf8"))
}
