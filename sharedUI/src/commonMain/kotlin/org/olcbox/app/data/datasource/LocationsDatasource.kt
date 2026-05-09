package org.olcbox.app.data.datasource

import io.ktor.client.HttpClient
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.get
import io.ktor.client.request.headers
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.olcbox.app.data.model.LocationBundleV4
import org.olcbox.app.data.model.LocationConfig
import org.olcbox.app.data.model.LocationEntry
import org.olcbox.app.data.model.LocationMetadata
import org.olcbox.app.data.model.SubscriptionMetadata
import org.olcbox.app.data.repository.LocationsRepository

interface LocationsDataSource {
    suspend fun loadLocationBundle(): LocationBundleV4?
    suspend fun saveLocationBundle(bundle: LocationBundleV4)
    suspend fun loadLegacyLocations(): List<Pair<String, String>>
    suspend fun loadLegacyActiveLocationId(): String?
}

private fun createLocationsHttpClient(): HttpClient {
    return HttpClient {
        expectSuccess = false

        install(HttpTimeout) {
            connectTimeoutMillis = 3_000
            requestTimeoutMillis = 8_000
            socketTimeoutMillis = 8_000
        }
    }
}

class LocationsRepositoryImpl(
    private val dataSource: LocationsDataSource,
    private val httpClient: HttpClient = createLocationsHttpClient()
) : LocationsRepository {
    private data class ImportSource(
        val content: String,
        val subscriptionUrl: String? = null
    )

    private data class ParsedOlcRtcUri(
        val location: LocationConfig,
        val mimo: String? = null
    )


    private val json = Json {
        ignoreUnknownKeys = true
        coerceInputValues = true
        encodeDefaults = true
        explicitNulls = false
        prettyPrint = true
    }

    override suspend fun getBundle(): LocationBundleV4 {
        val stored = dataSource.loadLocationBundle()?.normalized()
        if (stored != null && stored.locations.isNotEmpty()) return stored

        val legacy = migrateLegacyBundle()
        if (legacy.locations.isNotEmpty()) {
            dataSource.saveLocationBundle(legacy)
        }
        return legacy
    }

    override suspend fun saveBundle(bundle: LocationBundleV4) {
        dataSource.saveLocationBundle(bundle.normalized())
    }

    override suspend fun exportBundle(): String {
        return json.encodeToString(LocationBundleV4.serializer(), getBundle())
    }

    override suspend fun importText(text: String) {
        val source = resolveImportSource(text.trim()) ?: return
        val parsed = parseImport(source.content.trim(), source.subscriptionUrl) ?: return
        val merged = mergeImportedBundle(
            current = dataSource.loadLocationBundle(),
            imported = parsed.normalized()
        )
        dataSource.saveLocationBundle(merged.normalized())
    }

    override suspend fun refreshSubscriptions(): Int {
        val bundle = getBundle()
        if (bundle.locations.isEmpty()) return 0

        val groupedByUrl = bundle.locations
            .mapNotNull { entry -> entry.subscriptionUrl?.trim()?.takeIf { it.isNotBlank() }?.let { it to entry } }
            .groupBy({ it.first }, { it.second })
        if (groupedByUrl.isEmpty()) return 0

        val nonSubscriptionLocations = bundle.locations.filter { it.subscriptionUrl.isNullOrBlank() }.toMutableList()
        val usedStorageIds = nonSubscriptionLocations.mapTo(mutableSetOf()) { it.storageId }
        val activeBefore = bundle.activeLocationId
        var activeAfter = activeBefore

        groupedByUrl.forEach { (url, previousEntries) ->
            val source = resolveImportSource(url) ?: return@forEach
            val refreshed = parseImport(source.content, url)?.locations.orEmpty()
            if (refreshed.isEmpty()) return@forEach

            val reusedBySignature = previousEntries
                .groupBy { subscriptionSignature(it.location) }
                .mapValues { (_, entries) -> entries.toMutableList() }

            val reassigned = refreshed.mapIndexed { index, entry ->
                val signature = subscriptionSignature(entry.location)
                val reusedPool = reusedBySignature[signature]
                val reusedEntry = if (reusedPool.isNullOrEmpty()) null else reusedPool.removeAt(0)
                val storageId = reusedEntry?.storageId ?: uniqueStorageId(
                    base = "imported_${entry.location.storageSlug().ifBlank { "location_${index + 1}" }}",
                    used = usedStorageIds
                )
                if (activeBefore == reusedEntry?.storageId) {
                    activeAfter = storageId
                }
                entry.copy(storageId = storageId, subscriptionUrl = url).normalized()
            }

            if (activeBefore != null &&
                activeAfter == activeBefore &&
                previousEntries.any { it.storageId == activeBefore }
            ) {
                activeAfter = reassigned.firstOrNull()?.storageId
            }
            nonSubscriptionLocations += reassigned
        }

        if (groupedByUrl.isEmpty()) return 0

        dataSource.saveLocationBundle(
            bundle.copy(
                activeLocationId = activeAfter,
                locations = nonSubscriptionLocations
            ).normalized()
        )
        return groupedByUrl.size
    }

    override suspend fun saveLocation(storageId: String, location: LocationConfig) {
        val normalizedId = storageId.ifBlank { location.storageSlug() }
        val bundle = getBundle()
        val current = bundle.locations.firstOrNull { it.storageId == normalizedId }
        val entry = LocationEntry.from(
            storageId = normalizedId,
            location = location,
            subscriptionUrl = current?.subscriptionUrl,
            metadata = current?.metadata
        )
        val locations = bundle.locations
            .filterNot { it.storageId == entry.storageId } + entry

        dataSource.saveLocationBundle(
            bundle.copy(
                activeLocationId = entry.storageId,
                locations = locations
            ).normalized()
        )
    }

    override suspend fun loadLocation(storageId: String): LocationConfig? {
        return getBundle().locations.firstOrNull { it.storageId == storageId }?.location
    }

    override suspend fun deleteLocation(storageId: String) {
        val bundle = getBundle()
        dataSource.saveLocationBundle(
            bundle.copy(
                activeLocationId = bundle.activeLocationId?.takeUnless { it == storageId },
                locations = bundle.locations.filterNot { it.storageId == storageId }
            ).normalized()
        )
    }

    override suspend fun getAllLocations(): List<LocationEntry> {
        return getBundle().locations
    }

    override suspend fun getActiveLocationId(): String? {
        return getBundle().activeLocationId
    }

    override suspend fun setActiveLocationId(storageId: String?) {
        val bundle = getBundle()
        val nextActive = storageId?.takeIf { id -> bundle.locations.any { it.storageId == id } }
        dataSource.saveLocationBundle(bundle.copy(activeLocationId = nextActive).normalized())
    }

    override suspend fun getActiveLocation(): LocationEntry? {
        val bundle = getBundle()
        return bundle.locations.firstOrNull { it.storageId == bundle.activeLocationId }
    }

    private suspend fun resolveImportSource(text: String): ImportSource? {
        if (text.isBlank()) return null

        if (!text.isRemoteUrl()) {
            return ImportSource(content = text)
        }

        return downloadTextFromUrl(text)
            ?.trim()
            ?.takeIf { it.isNotBlank() }
            ?.let { ImportSource(content = it, subscriptionUrl = text.trim()) }
    }

    private suspend fun downloadTextFromUrl(url: String): String? {
        if (!url.isRemoteUrl()) return null

        val response = runCatching {
            httpClient.get(url) {
                headers {
                    append(
                        HttpHeaders.Accept,
                        "text/plain, text/markdown, application/octet-stream, */*"
                    )
                }
            }
        }.getOrNull() ?: return null

        if (response.status.value !in 200..299) {
            return null
        }

        return runCatching {
            response.bodyAsText()
        }.getOrNull()?.takeIf { it.isNotBlank() }
    }

    private fun String.isRemoteUrl(): Boolean {
        return trim().lowercase().startsWith("https://")
    }

    private suspend fun migrateLegacyBundle(): LocationBundleV4 {
        val legacy = dataSource.loadLegacyLocations().mapNotNull { (storageId, text) ->
            parseSingleLocation(text, storageId)
        }

        val active = dataSource.loadLegacyActiveLocationId()?.takeIf { id ->
            legacy.any { it.storageId == id }
        }

        return LocationBundleV4(
            activeLocationId = active,
            locations = legacy
        ).normalized()
    }

    private fun parseImport(text: String, subscriptionUrl: String? = null): LocationBundleV4? {
        parseOlcRtcText(text, subscriptionUrl)?.let { return it }

        if (!text.startsWith("{") || !text.endsWith("}")) return null

        val root = runCatching {
            json.parseToJsonElement(text).jsonObject
        }.getOrNull() ?: return null

        parseBundle(root, subscriptionUrl)?.let { return it }

        return parseSingleLocation(root, null, subscriptionUrl)?.let {
            LocationBundleV4(
                activeLocationId = it.storageId,
                locations = listOf(it)
            )
        }
    }

    private fun mergeImportedBundle(
        current: LocationBundleV4?,
        imported: LocationBundleV4
    ): LocationBundleV4 {
        val currentBundle = current?.normalized()
        if (currentBundle == null || currentBundle.locations.isEmpty()) {
            return imported
        }

        val importedByStorageId = imported.locations.associateBy { it.storageId }
        val existingStorageIds = currentBundle.locations.mapTo(mutableSetOf()) { it.storageId }

        val mergedLocations = currentBundle.locations
            .map { existing -> importedByStorageId[existing.storageId] ?: existing }
            .toMutableList()

        imported.locations.forEach { entry ->
            if (entry.storageId !in existingStorageIds) {
                mergedLocations += entry
            }
        }

        val importedActive = imported.activeLocationId
            ?.takeIf { id -> imported.locations.any { it.storageId == id } }

        val active = importedActive
            ?: currentBundle.activeLocationId?.takeIf { id -> mergedLocations.any { it.storageId == id } }
            ?: mergedLocations.firstOrNull()?.storageId

        return currentBundle.copy(
            activeLocationId = active,
            locations = mergedLocations
        )
    }

    private fun parseBundle(root: JsonObject, subscriptionUrl: String? = null): LocationBundleV4? {
        val locationsElement = root["locations"] ?: return null

        val locations = runCatching {
            locationsElement.jsonArray
        }.getOrNull()?.mapNotNull { element ->
            val item = element.jsonObjectOrNull() ?: return@mapNotNull null

            decodeLocationEntry(item, subscriptionUrl)?.let { return@mapNotNull it }

            val storageId = item.string("storage_id")
                ?: item.string("storageId")
                ?: item.string("id")?.let { "imported_${it.storageSlug()}" }

            parseSingleLocation(item, storageId, subscriptionUrl)
        } ?: return null

        val version = root["version"]?.jsonPrimitive?.intOrNull ?: 3
        if (version < 3 && locations.isEmpty()) return null

        return LocationBundleV4(
            activeLocationId = root.string("active_location_id")
                ?: root.string("activeLocationId"),
            locations = locations
        )
    }

    private fun parseSingleLocation(
        text: String,
        fallbackStorageId: String?,
        subscriptionUrl: String? = null
    ): LocationEntry? {
        val root = runCatching {
            json.parseToJsonElement(text).jsonObject
        }.getOrNull() ?: return null

        parseBundle(root, subscriptionUrl)?.let { bundle ->
            return bundle.normalized().locations.firstOrNull()
        }

        return parseSingleLocation(root, fallbackStorageId, subscriptionUrl)
    }

    private fun parseSingleLocation(
        root: JsonObject,
        fallbackStorageId: String?,
        subscriptionUrl: String? = null
    ): LocationEntry? {
        decodeLocationEntry(root, subscriptionUrl)?.let { return it }

        val source = root["location"]?.jsonObjectOrNull()
            ?: root["hysteria"]?.jsonObjectOrNull()
            ?: root

        val provider = firstNotBlank(
            source.string("bypass_provider"),
            source.string("bypassProvider"),
            source.string("provider"),
            root["turn"]?.jsonObjectOrNull()?.string("type"),
            root.string("bypass_provider"),
            root.string("bypassProvider"),
            root.string("provider")
        )

        val transportArgs = firstNotBlank(
            source.string("transport_args"),
            source.string("transportArgs"),
            source.string("args"),
            root.string("transport_args"),
            root.string("transportArgs"),
            root.string("args")
        )

        val vp8Fps = firstInt(
            source.int("vp8_fps"),
            source.int("vp8Fps"),
            root.int("vp8_fps"),
            root.int("vp8Fps"),
            transportArgInt(transportArgs, "-vp8-fps")
        ) ?: LocationConfig.DEFAULT_VP8_FPS

        val vp8Batch = firstInt(
            source.int("vp8_batch"),
            source.int("vp8Batch"),
            root.int("vp8_batch"),
            root.int("vp8Batch"),
            transportArgInt(transportArgs, "-vp8-batch")
        ) ?: LocationConfig.DEFAULT_VP8_BATCH

        val location = LocationConfig(
            name = firstNotBlank(source.string("name"), root.string("name")),
            id = firstNotBlank(
                source.string("id"),
                source.string("room_id"),
                source.string("server"),
                root.string("id")
            ),
            key = firstNotBlank(
                source.string("key"),
                source.string("encryption_key"),
                source.string("password"),
                root.string("key")
            ),
            clientId = firstNotBlank(
                source.string("client_id"),
                source.string("clientId"),
                root.string("client_id"),
                root.string("clientId"),
                LocationConfig.DEFAULT_CLIENT_ID
            ),
            bypassProvider = provider,
            transport = firstNotBlank(
                source.string("transport"),
                root.string("transport"),
                if (transportArgs.isNotBlank()) LocationConfig.TRANSPORT_VP8CHANNEL else null
            ),
            vp8Fps = vp8Fps,
            vp8Batch = vp8Batch
        ).normalized()

        if (!location.isComplete()) return null

        val storageId = firstNotBlank(
            fallbackStorageId,
            root.string("storage_id"),
            root.string("storageId"),
            source.string("storage_id"),
            source.string("storageId"),
            "imported_${location.storageSlug()}"
        )

        return LocationEntry.from(storageId, location, subscriptionUrl = subscriptionUrl)
    }

    private fun parseOlcRtcText(text: String, subscriptionUrl: String? = null): LocationBundleV4? {
        if (!text.contains(OLCRTC_URI_PREFIX)) return null

        val subscriptionFields = linkedMapOf<String, String>()
        val locations = mutableListOf<Pair<ParsedOlcRtcUri, MutableMap<String, String>>>()
        var localFields: MutableMap<String, String>? = null

        text.lineSequence()
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .forEach { line ->
                when {
                    line.startsWith(OLCRTC_URI_PREFIX) -> {
                        parseOlcRtcUri(line)?.let { parsed ->
                            val fields = linkedMapOf<String, String>()
                            locations += parsed to fields
                            localFields = fields
                        }
                    }

                    line.startsWith("##") && locations.isNotEmpty() -> {
                        val (key, value) = parseSubscriptionField(
                            line.removePrefix("##")
                        ) ?: return@forEach

                        localFields?.set(key, value)
                    }

                    line.startsWith("#") -> {
                        val (key, value) = parseSubscriptionField(
                            line.removePrefix("#")
                        ) ?: return@forEach

                        subscriptionFields[key] = value
                    }
                }
            }

        if (locations.isEmpty()) return null

        val subscriptionMetadata = buildSubscriptionMetadata(subscriptionFields)
        val usedStorageIds = mutableSetOf<String>()

        val entries = locations.mapIndexed { index, (parsed, fields) ->
            val metadata = buildLocationMetadata(
                fields = fields,
                mimo = parsed.mimo,
                subscription = subscriptionMetadata
            )
            val location = parsed.location.copy(
                name = firstNotBlank(
                    metadata?.name,
                    parsed.mimo,
                    parsed.location.name
                )
            ).normalized()
            val base = location.storageSlug().ifBlank { "location_${index + 1}" }
            val storageId = uniqueStorageId("imported_$base", usedStorageIds)
            LocationEntry.from(
                storageId = storageId,
                location = location,
                subscriptionUrl = subscriptionUrl,
                metadata = metadata
            )
        }

        return LocationBundleV4(
            activeLocationId = entries.firstOrNull()?.storageId,
            locations = entries
        )
    }

    private fun parseOlcRtcUri(line: String): ParsedOlcRtcUri? {
        val payload = line.removePrefix(OLCRTC_URI_PREFIX)

        val transportMarker = payload.indexOf('?')
        val roomMarker = payload.indexOf('@', startIndex = transportMarker + 1)
        val keyMarker = payload.indexOf('#', startIndex = roomMarker + 1)

        if (transportMarker <= 0 || roomMarker <= transportMarker || keyMarker <= roomMarker) {
            return null
        }

        val clientMarker = payload
            .indexOf('%', startIndex = keyMarker + 1)
            .takeIf { it >= 0 }

        val mimoMarker = payload
            .indexOf('$', startIndex = keyMarker + 1)
            .takeIf { it >= 0 }

        val keyEnd = listOfNotNull(clientMarker, mimoMarker).minOrNull() ?: payload.length
        val clientEnd = mimoMarker ?: payload.length

        val carrier = payload.substring(0, transportMarker).trim()
        val transport = payload.substring(transportMarker + 1, roomMarker).trim()
        val roomId = payload.substring(roomMarker + 1, keyMarker).trim()
        val key = payload.substring(keyMarker + 1, keyEnd).trim()

        val clientId = clientMarker
            ?.let { payload.substring(it + 1, clientEnd).trim() }
            .orEmpty()
            .ifBlank { LocationConfig.DEFAULT_CLIENT_ID }

        val mimo = mimoMarker
            ?.let { payload.substring(it + 1) }
            .orEmpty()
            .trim()

        val location = LocationConfig(
            name = mimo.ifBlank { roomId },
            id = roomId,
            key = key,
            clientId = clientId,
            bypassProvider = carrier,
            transport = transport
        ).normalized()

        return location
            .takeIf { it.isComplete() }
            ?.let { ParsedOlcRtcUri(it, mimo.takeIf { value -> value.isNotBlank() }) }
    }

    private fun buildSubscriptionMetadata(fields: Map<String, String>): SubscriptionMetadata? {
        return SubscriptionMetadata(
            name = fields["name"],
            update = fields["update"],
            refresh = fields["refresh"],
            color = fields["color"],
            icon = fields["icon"],
            used = fields["used"],
            available = fields["available"]
        ).normalized().takeUnless { it.isEmpty() }
    }

    private fun buildLocationMetadata(
        fields: Map<String, String>,
        mimo: String?,
        subscription: SubscriptionMetadata?
    ): LocationMetadata? {
        return LocationMetadata(
            name = fields["name"],
            color = fields["color"],
            icon = fields["icon"],
            used = fields["used"],
            available = fields["available"],
            ip = fields["ip"],
            comment = fields["comment"],
            mimo = mimo,
            subscription = subscription
        ).normalized().takeUnless { it.isEmpty() }
    }

    private fun parseSubscriptionField(value: String): Pair<String, String>? {
        val separator = value.indexOf(':')
        if (separator <= 0) return null

        val key = value.substring(0, separator).trim().lowercase()
        val fieldValue = value.substring(separator + 1).trim()

        return key to fieldValue
    }

    private fun uniqueStorageId(base: String, used: MutableSet<String>): String {
        val normalizedBase = base.storageSlug()
        var candidate = normalizedBase
        var suffix = 2

        while (!used.add(candidate)) {
            candidate = "${normalizedBase}_$suffix"
            suffix += 1
        }

        return candidate
    }

    private fun decodeLocationEntry(root: JsonObject, subscriptionUrl: String? = null): LocationEntry? {
        return runCatching {
            json.decodeFromJsonElement(LocationEntry.serializer(), root)
                .let { entry ->
                    if (entry.subscriptionUrl.isNullOrBlank() && !subscriptionUrl.isNullOrBlank()) {
                        entry.copy(subscriptionUrl = subscriptionUrl)
                    } else {
                        entry
                    }
                }
                .normalized()
                .takeIf { it.location.isComplete() }
        }.getOrNull()
    }

    private fun subscriptionSignature(location: LocationConfig): String {
        val normalized = location.normalized()
        return listOf(
            normalized.bypassProvider,
            normalized.transport,
            normalized.id,
            normalized.clientId
        ).joinToString("|")
    }

    private fun LocationConfig.storageSlug(): String {
        return displayName().ifBlank { id }.storageSlug()
    }

    private fun String.storageSlug(): String {
        return lowercase()
            .replace(Regex("[^a-z0-9_-]+"), "_")
            .trim('_')
            .take(32)
            .ifBlank { "location" }
    }

    private fun JsonObject.string(name: String): String? {
        return (this[name] as? JsonPrimitive)?.contentOrNull
    }

    private fun JsonObject.int(name: String): Int? {
        return (this[name] as? JsonPrimitive)?.intOrNull
    }

    private fun JsonElement.jsonObjectOrNull(): JsonObject? {
        return runCatching { jsonObject }.getOrNull()
    }

    private fun firstNotBlank(vararg values: String?): String {
        return values.firstOrNull { !it.isNullOrBlank() } ?: ""
    }

    private fun firstInt(vararg values: Int?): Int? {
        return values.firstOrNull { it != null }
    }

    private fun transportArgInt(args: String, name: String): Int? {
        if (args.isBlank()) return null

        val parts = args.split(Regex("\\s+")).filter { it.isNotBlank() }
        val index = parts.indexOf(name)

        return parts.getOrNull(index + 1)?.toIntOrNull()
    }

    private companion object {
        const val OLCRTC_URI_PREFIX = "olcrtc://"
    }
}
