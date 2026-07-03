package io.zugzwang.tessera

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytes
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.RoutingCall
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory
import java.util.Base64

/** The watermark header on single-key reads. */
const val SEQUENCE_HEADER = "X-Tessera-Sequence"

@Serializable
data class CollectionResponse(
    @SerialName("as_of_sequence") val asOfSequence: Long,
    val entries: List<EntryResponse>,
)

@Serializable
data class EntryResponse(val key: String, val value: String)

private val logger = LoggerFactory.getLogger("io.zugzwang.tessera.ReadApi")

fun Route.readApi(stateView: StateView) {
    route("/v1/collections/{collection}") {
        get {
            val state = call.view { stateView.collection(call.parameters["collection"]!!) } ?: return@get
            val encoder = Base64.getEncoder()
            val entries = state.entries.map { EntryResponse(it.key, encoder.encodeToString(it.value)) }
            call.respond(CollectionResponse(state.asOfSequence, entries))
        }

        get("/keys/{key}") {
            val state = call.view {
                stateView.key(call.parameters["collection"]!!, call.parameters["key"]!!)
            } ?: return@get
            call.response.header(SEQUENCE_HEADER, state.asOfSequence.toString())
            val value = state.value
                ?: return@get call.respondText("key not found", status = HttpStatusCode.NotFound)
            call.respondBytes(value, ContentType.Application.OctetStream)
        }
    }
}

/**
 * Runs a state-view query; responds 503 (and returns null) when the view is
 * unreachable or the projector has never checkpointed.
 */
private suspend fun <T : Any> RoutingCall.view(query: () -> T?): T? {
    val state = runCatching(query).getOrElse { cause ->
        logger.warn("state view query failed", cause)
        null
    }
    if (state == null) {
        respondText("state view unavailable, retry", status = HttpStatusCode.ServiceUnavailable)
    }
    return state
}
