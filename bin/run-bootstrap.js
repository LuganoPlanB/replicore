#!/usr/bin/env node
import HyperDHT from "hyperdht"

const host = process.env.HOLEPUNCH_BOOTSTRAP_HOST ?? "127.0.0.1"
const port = Number(process.env.HOLEPUNCH_BOOTSTRAP_PORT ?? "49737")

const node = HyperDHT.bootstrapper(port, host)

await node.ready()

console.log(
  JSON.stringify(
    {
      type: "bootstrap-ready",
      host,
      port
    },
    null,
    2
  )
)

const shutdown = async () => {
  await node.destroy()
  process.exit(0)
}

process.on("SIGINT", () => void shutdown())
process.on("SIGTERM", () => void shutdown())
