import os from "node:os"

/**
 * Normalize one Node network-interface family value.
 *
 * @param {string | number | undefined} family
 * @returns {"IPv4" | "IPv6" | null}
 */
export function normalizeInterfaceFamily(family) {
  if (family === "IPv4" || family === 4) return "IPv4"
  if (family === "IPv6" || family === 6) return "IPv6"
  return null
}

/**
 * Convert Node's `os.networkInterfaces()` result into stable setup records.
 *
 * @param {NodeJS.Dict<os.NetworkInterfaceInfo[]>} interfaces
 */
export function normalizeNetworkInterfaces(interfaces) {
  const records = []

  for (const [name, candidates] of Object.entries(interfaces ?? {})) {
    for (const candidate of candidates ?? []) {
      const family = normalizeInterfaceFamily(candidate?.family)

      if (!family) continue

      records.push({
        name,
        family,
        address: candidate?.address ?? "",
        netmask: candidate?.netmask ?? "",
        mac: candidate?.mac ?? "",
        internal: Boolean(candidate?.internal),
        cidr: candidate?.cidr ?? null,
        eligibleForBind: !candidate?.internal && Boolean(candidate?.address)
      })
    }
  }

  records.sort(compareNetworkInterfaceRecords)
  return records
}

/**
 * List normalized network interfaces from the current machine.
 */
export function listNetworkInterfaces() {
  return normalizeNetworkInterfaces(os.networkInterfaces())
}

/**
 * Sort setup interface records with likely bind candidates first.
 *
 * @param {{
 *   name: string,
 *   family: "IPv4" | "IPv6",
 *   address: string,
 *   internal: boolean
 * }} left
 * @param {{
 *   name: string,
 *   family: "IPv4" | "IPv6",
 *   address: string,
 *   internal: boolean
 * }} right
 */
export function compareNetworkInterfaceRecords(left, right) {
  if (left.internal !== right.internal) {
    return left.internal ? 1 : -1
  }

  if (left.family !== right.family) {
    return left.family === "IPv4" ? -1 : 1
  }

  if (left.name !== right.name) {
    return left.name.localeCompare(right.name)
  }

  return left.address.localeCompare(right.address)
}
