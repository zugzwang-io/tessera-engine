package io.zugzwang.tessera

import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Appends a change durably to the log and returns its sequence number.
 * Resuming before the durable commit would let fan-out emit a phantom
 * (CLAUDE.md invariant 4), so implementations must only return once the
 * record is committed.
 */
interface ChangeLog {
    suspend fun append(change: Change): Long
}

class KafkaChangeLog(
    private val producer: Producer<String, ByteArray>,
    private val topic: String,
) : ChangeLog {

    override suspend fun append(change: Change): Long {
        val record = ProducerRecord(topic, change.collection, EnvelopeV1.encode(change))
        return suspendCancellableCoroutine { continuation ->
            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    continuation.resumeWithException(exception)
                } else {
                    continuation.resume(metadata.offset())
                }
            }
        }
    }

    companion object {
        fun fromEnv(): KafkaChangeLog {
            val bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
            val topic = System.getenv("TESSERA_LOG_TOPIC") ?: "tessera-log"
            return KafkaChangeLog(producer(bootstrap), topic)
        }

        fun producer(bootstrapServers: String): KafkaProducer<String, ByteArray> {
            val config = mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                // Ack only on durable commit (CLAUDE.md invariant 4)
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
                // Fail fast so a dead broker surfaces as 503, not a hung request
                ProducerConfig.MAX_BLOCK_MS_CONFIG to "5000",
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to "5000",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to "15000",
            )
            return KafkaProducer(config, StringSerializer(), ByteArraySerializer())
        }
    }
}
