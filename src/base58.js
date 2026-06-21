import { base58_to_binary, binary_to_base58 } from "base58-js"

const HEX_RE = /^[0-9a-fA-F]+$/

export function base58Encode(buf) {
  const bytes = new Uint8Array(Buffer.isBuffer(buf) ? buf : Buffer.from(buf))
  return binary_to_base58(bytes)
}

export function base58Decode(str) {
  return Buffer.from(base58_to_binary(str))
}

export function decodeHexOrBase58(str) {
  if (typeof str !== "string" || str.length === 0) {
    throw new Error("decodeHexOrBase58: expected a non-empty string")
  }
  if (HEX_RE.test(str)) {
    return Buffer.from(str, "hex")
  }
  try {
    return base58Decode(str)
  } catch (cause) {
    throw new Error("decodeHexOrBase58: value must be hex or base58", { cause })
  }
}
