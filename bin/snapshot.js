#!/usr/bin/env node
import { readSnapshotFile, writeSnapshotFile } from "../src/index.js"

const [command, baseUrl, token, filePath] = process.argv.slice(2)

if (!command || !baseUrl || !token || !filePath) {
  console.error("Usage: node bin/snapshot.js <export|import> <baseUrl> <token> <filePath>")
  process.exit(1)
}

if (command === "export") {
  const response = await fetch(new URL("/admin/snapshot", baseUrl), {
    headers: {
      authorization: `Bearer ${token}`
    }
  })

  if (!response.ok) {
    console.error(`Snapshot export failed: ${response.status} ${await response.text()}`)
    process.exit(1)
  }

  const snapshot = await response.json()
  const saved = await writeSnapshotFile(filePath, snapshot)
  console.log(JSON.stringify({ type: "snapshot-exported", file: saved }, null, 2))
  process.exit(0)
}

if (command === "import") {
  const snapshot = await readSnapshotFile(filePath)
  const response = await fetch(new URL("/admin/snapshot/import", baseUrl), {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json"
    },
    body: JSON.stringify(snapshot)
  })

  if (!response.ok) {
    console.error(`Snapshot import failed: ${response.status} ${await response.text()}`)
    process.exit(1)
  }

  console.log(JSON.stringify({ type: "snapshot-imported", file: filePath }, null, 2))
  process.exit(0)
}

console.error(`Unknown command: ${command}`)
process.exit(1)
