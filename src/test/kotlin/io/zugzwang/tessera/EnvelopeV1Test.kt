package io.zugzwang.tessera

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class EnvelopeV1Test {

    @Test
    fun `round-trips a multi-entry change`() {
        val entries = listOf(
            Change.Entry("a", byteArrayOf(1, 2, 3)),
            Change.Entry("emoji-🔑", ByteArray(0)),
            Change.Entry("b", ByteArray(1024) { it.toByte() }),
        )
        val decoded = EnvelopeV1.decode(EnvelopeV1.encode(Change("orders", entries)))

        assertEquals(entries.map { it.key }, decoded.map { it.key })
        entries.zip(decoded).forEach { (expected, actual) -> assertContentEquals(expected.value, actual.value) }
    }

    @Test
    fun `round-trips tombstone entries`() {
        val entries = listOf(
            Change.Entry("gone", ByteArray(0), tombstone = true),
            Change.Entry("kept", byteArrayOf(7)),
        )
        val decoded = EnvelopeV1.decode(EnvelopeV1.encode(Change("orders", entries)))

        assertEquals(listOf(true, false), decoded.map { it.tombstone })
        assertContentEquals(byteArrayOf(7), decoded[1].value)
    }

    @Test
    fun `refuses to encode a tombstone carrying a value`() {
        val change = Change("orders", listOf(Change.Entry("a", byteArrayOf(1), tombstone = true)))
        assertFailsWith<IllegalArgumentException> { EnvelopeV1.encode(change) }
    }

    @Test
    fun `rejects unknown entry flags`() {
        val bytes = EnvelopeV1.encode(Change("orders", listOf(Change.Entry("a", byteArrayOf(1)))))
        // flags byte of the first entry: version(1) + count(4) + key length(4) + key "a"(1)
        bytes[10] = 2
        assertFailsWith<IllegalArgumentException> { EnvelopeV1.decode(bytes) }
    }

    @Test
    fun `rejects an unknown version`() {
        val bytes = EnvelopeV1.encode(Change("orders", listOf(Change.Entry("a", byteArrayOf(1)))))
        bytes[0] = 99
        assertFailsWith<IllegalArgumentException> { EnvelopeV1.decode(bytes) }
    }
}
