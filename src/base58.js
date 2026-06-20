import { base58_to_binary, binary_to_base58 } from "base58-js"

export function base58Encode(buf) {
  const bytes = new Uint8Array(Buffer.isBuffer(buf) ? buf : Buffer.from(buf))
  return binary_to_base58(bytes)
}

export function base58Decode(str) {
  return Buffer.from(base58_to_binary(str))
}
