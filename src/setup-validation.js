/**
 * Normalize one machine-ID derivation request from the setup UI.
 *
 * @param {{
 *   clusterSecret: string,
 *   machineIdentity: string
 * }} input
 */
export function normalizeSetupMachineIdInput(input) {
  if (!input || typeof input !== "object") {
    throw badRequest("Request body must be an object")
  }

  const clusterSecret = requireHex(input.clusterSecret, "clusterSecret", 32)
  const machineIdentity = requireTrimmedString(input.machineIdentity, "machineIdentity")

  return {
    clusterSecret,
    machineIdentity
  }
}

/**
 * Normalize one persisted setup draft.
 *
 * @param {{
 *   selectedInterface: string,
 *   bindHost: string,
 *   clusterSecret: string,
 *   machineIdentity: string,
 *   machineId: string,
 *   updatedAt?: string,
 *   schemaVersion?: number
 * }} input
 */
export function normalizeSetupDraft(input) {
  if (!input || typeof input !== "object") {
    throw badRequest("Request body must be an object")
  }

  const updatedAt = input.updatedAt ?? new Date().toISOString()
  if (typeof updatedAt !== "string" || updatedAt.length === 0) {
    throw badRequest("updatedAt must be a non-empty string")
  }

  const schemaVersion = input.schemaVersion ?? 1
  if (!Number.isInteger(schemaVersion) || schemaVersion <= 0) {
    throw badRequest("schemaVersion must be a positive integer")
  }

  return {
    schemaVersion,
    updatedAt,
    selectedInterface: requireTrimmedString(input.selectedInterface, "selectedInterface"),
    bindHost: requireTrimmedString(input.bindHost, "bindHost"),
    clusterSecret: requireHexString(input.clusterSecret, "clusterSecret", 32),
    machineIdentity: requireTrimmedString(input.machineIdentity, "machineIdentity"),
    machineId: requireHexString(input.machineId, "machineId", 32)
  }
}

function requireTrimmedString(value, field) {
  if (typeof value !== "string") {
    throw badRequest(`${field} must be a non-empty string`)
  }

  const normalized = value.trim()
  if (normalized.length === 0) {
    throw badRequest(`${field} must be a non-empty string`)
  }

  return normalized
}

function requireHex(value, field, exactLength) {
  if (typeof value !== "string" || !/^[0-9a-fA-F]+$/.test(value)) {
    throw badRequest(`${field} must be a hex string`)
  }

  const buffer = Buffer.from(value, "hex")
  if (buffer.length !== exactLength) {
    throw badRequest(`${field} must decode to ${exactLength} bytes`)
  }

  return buffer
}

function requireHexString(value, field, exactLength) {
  requireHex(value, field, exactLength)
  return value.toLowerCase()
}

function badRequest(message) {
  const error = new Error(message)
  error.statusCode = 400
  return error
}
