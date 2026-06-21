import { base58Decode } from "./base58.js"

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

  const clusterSecret = requireBase58Bytes(input.clusterSecret, "clusterSecret", 32)
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

  const initCluster = input.initCluster === true

  const role = input.role || (initCluster ? "voter" : "learner")
  if (role !== "voter" && role !== "learner") {
    throw badRequest("role must be either voter or learner")
  }
  if (initCluster && role !== "voter") {
    throw badRequest("initCluster may only be used with voter role")
  }

  return {
    schemaVersion,
    updatedAt,
    initCluster,
    role,
    selectedInterface: requireTrimmedString(input.selectedInterface, "selectedInterface"),
    bindHost: requireTrimmedString(input.bindHost, "bindHost"),
    clusterSecret: requireBase58String(input.clusterSecret, "clusterSecret", 32),
    machineIdentity: requireTrimmedString(input.machineIdentity, "machineIdentity"),
    machineId: requireBase58String(input.machineId, "machineId", 32)
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

function requireBase58String(value, field, exactLength) {
  const decoded = base58Decode(value)
  if (decoded.length !== exactLength) {
    throw badRequest(`${field} must decode to ${exactLength} bytes`)
  }
  return value
}

function requireBase58Bytes(value, field, exactLength) {
  const decoded = base58Decode(value)
  if (decoded.length !== exactLength) {
    throw badRequest(`${field} must decode to ${exactLength} bytes`)
  }
  return decoded
}

function badRequest(message) {
  const error = new Error(message)
  error.statusCode = 400
  return error
}
