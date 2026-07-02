package io.zugzwang.tessera

import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.RoutingCall
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import kotlinx.serialization.Serializable
import java.util.Base64

/**
 * A change is the unit of atomicity: all of its entries are committed to the
 * collection's log as exactly one record and applied as one unit everywhere,
 * or not at all. Keys must be unique within a change — a shadowed entry could
 * never be observed, so a duplicate is a malformed request, and rejecting now
 * is backward-compatible to relax later. Atomicity never spans collections.
 *
 * Bounded by the broker's max message size, since one change = one record.
 */
const val MAX_CHANGE_BYTES = 1 shl 20

@Serializable
data class ChangeRequest(val entries: List<ChangeEntry>)

/** [value] is base64-encoded opaque bytes; the engine never interprets them. */
@Serializable
data class ChangeEntry(val key: String, val value: String)

data class Change(val collection: String, val entries: List<Entry>) {
    data class Entry(val key: String, val value: ByteArray)
}

fun Route.writeApi() {
    route("/v1/collections/{collection}") {
        post("/changes") {
            val request = runCatching { call.receive<ChangeRequest>() }.getOrElse {
                return@post call.badRequest("malformed request body")
            }
            if (request.entries.isEmpty()) {
                return@post call.badRequest("a change must contain at least one entry")
            }
            if (request.entries.any { it.key.isBlank() }) {
                return@post call.badRequest("entry keys must not be blank")
            }
            if (request.entries.size != request.entries.distinctBy { it.key }.size) {
                return@post call.badRequest("entry keys must be unique within a change")
            }
            val entries = request.entries.map {
                val value = runCatching { Base64.getDecoder().decode(it.value) }.getOrElse {
                    return@post call.badRequest("entry values must be base64")
                }
                Change.Entry(it.key, value)
            }
            call.accept(Change(call.collection(), entries))
        }

        put("/keys/{key}") {
            val value = call.receive<ByteArray>()
            val entry = Change.Entry(call.parameters["key"]!!, value)
            call.accept(Change(call.collection(), listOf(entry)))
        }
    }
}

private suspend fun RoutingCall.accept(change: Change) {
    val size = change.entries.sumOf { it.key.length + it.value.size }
    if (size > MAX_CHANGE_BYTES) {
        return respondText("change exceeds $MAX_CHANGE_BYTES bytes", status = HttpStatusCode.PayloadTooLarge)
    }
    // Commit to the log lands in a follow-up PR; for now the change is validated and dropped.
    respond(HttpStatusCode.Accepted)
}

private fun RoutingCall.collection(): String = parameters["collection"]!!

private suspend fun RoutingCall.badRequest(message: String) =
    respondText(message, status = HttpStatusCode.BadRequest)
