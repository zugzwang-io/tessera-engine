package io.zugzwang.tessera

import java.nio.ByteBuffer

/**
 * Minimal v1 framing for one change record: version byte, entry count, then
 * per-entry key, flags, and value. Keys are UTF-8; values are opaque bytes
 * the engine never interprets. A tombstone entry deletes its key from the
 * latest-state view (the log itself retains full history).
 *
 * Provisional: the full envelope design (tenant, epoch, write-id, headers) is
 * an open issue in docs/engine-design.md. The leading version byte is the ABI
 * gate — the ABI freeze begins at the first real deployment; until then v1
 * may be amended in place, after that layouts bump the version and are never
 * reinterpreted.
 */
object EnvelopeV1 {
    const val VERSION: Byte = 1

    private const val FLAG_TOMBSTONE: Byte = 1

    fun encode(change: Change): ByteArray {
        val keys = change.entries.map { it.key.encodeToByteArray() }
        // 1 version byte + 4-byte entry count, then per entry: 4-byte key length,
        // key bytes, 1 flags byte, 4-byte value length, value bytes (the prefixes
        // and the flags byte are the 9)
        val size = 1 + 4 + change.entries.indices.sumOf { 9 + keys[it].size + change.entries[it].value.size }
        val buf = ByteBuffer.allocate(size)
        buf.put(VERSION)
        buf.putInt(change.entries.size)
        change.entries.forEachIndexed { i, entry ->
            require(!entry.tombstone || entry.value.isEmpty()) { "a tombstone entry must not carry a value" }
            buf.putInt(keys[i].size).put(keys[i])
            buf.put(if (entry.tombstone) FLAG_TOMBSTONE else 0)
            buf.putInt(entry.value.size).put(entry.value)
        }
        return buf.array()
    }

    fun decode(bytes: ByteArray): List<Change.Entry> {
        val buf = ByteBuffer.wrap(bytes)
        val version = buf.get()
        require(version == VERSION) { "unsupported envelope version $version" }
        return List(buf.int) {
            val key = ByteArray(buf.int).also(buf::get).decodeToString()
            val flags = buf.get()
            require(flags in 0..FLAG_TOMBSTONE) { "unsupported entry flags $flags" }
            val value = ByteArray(buf.int).also(buf::get)
            Change.Entry(key, value, tombstone = flags == FLAG_TOMBSTONE)
        }
    }
}
