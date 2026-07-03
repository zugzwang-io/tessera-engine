package io.zugzwang.tessera

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class EnvelopeV1Test {

    @Test
    fun `round-trips a multi-entry change`() {
        val entries = listOf(
            Change.Put("a", byteArrayOf(1, 2, 3)),
            Change.Put("emoji-🔑", ByteArray(0)),
            Change.Put("b", ByteArray(1024) { it.toByte() }),
        )
        val decoded = EnvelopeV1.decode(EnvelopeV1.encode(Change("orders", entries)))

        assertEquals(entries.map { it.key }, decoded.map { it.key })
        entries.zip(decoded).forEach { (expected, actual) ->
            assertContentEquals(expected.value, assertIs<Change.Put>(actual).value)
        }
    }

    @Test
    fun `round-trips tombstone entries`() {
        val entries = listOf(Change.Tombstone("gone"), Change.Put("kept", byteArrayOf(7)))
        val decoded = EnvelopeV1.decode(EnvelopeV1.encode(Change("orders", entries)))

        assertIs<Change.Tombstone>(decoded[0])
        assertEquals("gone", decoded[0].key)
        assertContentEquals(byteArrayOf(7), assertIs<Change.Put>(decoded[1]).value)
    }

    @Test
    fun `rejects unknown entry flags`() {
        val bytes = EnvelopeV1.encode(Change("orders", listOf(Change.Put("a", byteArrayOf(1)))))
        // flags byte of the first entry: version(1) + count(4) + key length(4) + key "a"(1)
        bytes[10] = 2
        assertFailsWith<IllegalArgumentException> { EnvelopeV1.decode(bytes) }
    }

    @Test
    fun `rejects an unknown version`() {
        val bytes = EnvelopeV1.encode(Change("orders", listOf(Change.Put("a", byteArrayOf(1)))))
        bytes[0] = 99
        assertFailsWith<IllegalArgumentException> { EnvelopeV1.decode(bytes) }
    }
}
