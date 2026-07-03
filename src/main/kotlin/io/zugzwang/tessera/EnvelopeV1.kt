package io.zugzwang.tessera

import java.nio.ByteBuffer

/**
 * Minimal v1 framing for one change record: version byte, entry count, then
 * per-entry key and flags, followed by the value for puts only — tombstones
 * carry no value bytes at all. Keys are UTF-8; values are opaque bytes the
 * engine never interprets. A tombstone deletes its key from the latest-state
 * view (the log itself retains full history).
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
        // 1 version byte + 4-byte entry count, then per entry: 4-byte key
        // length, key bytes, 1 flags byte, and for puts a 4-byte value length
        // plus value bytes
        val size = 1 + 4 + change.entries.indices.sumOf {
            val valueSize = when (val entry = change.entries[it]) {
                is Change.Put -> 4 + entry.value.size
                is Change.Tombstone -> 0
            }
            5 + keys[it].size + valueSize
        }
        val buf = ByteBuffer.allocate(size)
        buf.put(VERSION)
        buf.putInt(change.entries.size)
        change.entries.forEachIndexed { i, entry ->
            buf.putInt(keys[i].size).put(keys[i])
            when (entry) {
                is Change.Put -> buf.put(0).putInt(entry.value.size).put(entry.value)
                is Change.Tombstone -> buf.put(FLAG_TOMBSTONE)
            }
        }
        return buf.array()
    }

    fun decode(bytes: ByteArray): List<Change.Entry> {
        val buf = ByteBuffer.wrap(bytes)
        val version = buf.get()
        require(version == VERSION) { "unsupported envelope version $version" }
        return List(buf.int) {
            val key = ByteArray(buf.int).also(buf::get).decodeToString()
            when (val flags = buf.get()) {
                FLAG_TOMBSTONE -> Change.Tombstone(key)
                0.toByte() -> Change.Put(key, ByteArray(buf.int).also(buf::get))
                else -> throw IllegalArgumentException("unsupported entry flags $flags")
            }
        }
    }
}
