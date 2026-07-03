package io.zugzwang.tessera

import java.nio.ByteBuffer

/**
 * Minimal v1 framing for one change record: version byte, entry count, then
 * length-prefixed (key, value) pairs. Keys are UTF-8; values are opaque bytes
 * the engine never interprets.
 *
 * Provisional: the full envelope design (tenant, epoch, write-id, headers) is
 * an open issue in docs/engine-design.md. The leading version byte is the ABI
 * gate — later layouts bump it; existing records are never reinterpreted.
 */
object EnvelopeV1 {
    const val VERSION: Byte = 1

    fun encode(change: Change): ByteArray {
        val keys = change.entries.map { it.key.encodeToByteArray() }
        val size = 1 + 4 + change.entries.indices.sumOf { 8 + keys[it].size + change.entries[it].value.size }
        val buf = ByteBuffer.allocate(size)
        buf.put(VERSION)
        buf.putInt(change.entries.size)
        change.entries.forEachIndexed { i, entry ->
            buf.putInt(keys[i].size).put(keys[i])
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
            val value = ByteArray(buf.int).also(buf::get)
            Change.Entry(key, value)
        }
    }
}
