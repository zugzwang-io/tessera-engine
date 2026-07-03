package io.zugzwang.tessera

/**
 * All environment access lives here, read once at boot; everything else
 * takes plain values. Defaults match the local compose stack.
 */
data class Config(
    val port: Int,
    val kafkaBootstrapServers: String,
    val logTopic: String,
    val postgresUrl: String,
    val postgresUser: String,
    val postgresPassword: String,
) {
    companion object {
        fun fromEnv(): Config = Config(
            port = System.getenv("PORT")?.toInt() ?: 8080,
            kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092",
            logTopic = System.getenv("TESSERA_LOG_TOPIC") ?: "tessera-log",
            postgresUrl = System.getenv("POSTGRES_URL") ?: "jdbc:postgresql://localhost:5432/tessera",
            postgresUser = System.getenv("POSTGRES_USER") ?: "tessera",
            postgresPassword = System.getenv("POSTGRES_PASSWORD") ?: "tessera",
        )
    }
}
