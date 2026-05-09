package org.olcbox.app.data.datasource

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import org.olcbox.app.data.model.LocationBundleV4
import org.olcbox.app.data.model.LocationConfig
import org.olcbox.app.data.model.LocationEntry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LocationsRepositoryImplTest {

    @Test
    fun exportsAndImportsBundleV4WithActiveLocation() = runTest {
        val first = LocationEntry.from(
            "amsterdam",
            LocationConfig("Amsterdam", "room-a", "key-a", LocationConfig.PROVIDER_JAZZ)
        )
        val second = LocationEntry.from(
            "berlin",
            LocationConfig("Berlin", "room-b", "key-b", LocationConfig.PROVIDER_TELEMOST)
        )
        val source = FakeLocationsDataSource(
            stored = LocationBundleV4(
                activeLocationId = "berlin",
                locations = listOf(first, second)
            )
        )
        val exported = LocationsRepositoryImpl(source).exportBundle()
        assertTrue("\"version\": 4" in exported)
        assertTrue("\"endpoint\"" in exported)
        assertTrue("\"carrier\"" in exported)
        assertTrue("\"bypass_provider\"" !in exported)
        val importedSource = FakeLocationsDataSource()

        LocationsRepositoryImpl(importedSource).importText(exported)

        val imported = importedSource.stored
        assertNotNull(imported)
        assertEquals(4, imported.version)
        assertEquals("berlin", imported.activeLocationId)
        assertEquals(listOf("amsterdam", "berlin"), imported.locations.map { it.storageId })
        assertEquals(
            LocationConfig.PROVIDER_TELEMOST,
            imported.locations[1].location.bypassProvider
        )
    }

    @Test
    fun migratesLegacyLocationsAndPreservesActiveSelection() = runTest {
        val source = FakeLocationsDataSource(
            legacy = listOf(
                "legacy_a" to """{"name":"A","server":"room-a","password":"key-a","provider":"jazz"}""",
                "legacy_b" to """{"name":"B","server":"room-b","password":"key-b","turn":{"type":"wbstream"}}"""
            ),
            legacyActive = "legacy_b"
        )
        val bundle = LocationsRepositoryImpl(source).getBundle()

        assertEquals("legacy_b", bundle.activeLocationId)
        assertEquals(listOf("legacy_a", "legacy_b"), bundle.locations.map { it.storageId })
        assertEquals(LocationConfig.PROVIDER_WB_STREAM, bundle.locations[1].location.bypassProvider)
        assertEquals(bundle, source.stored)
    }

    @Test
    fun normalizesStoredWbStreamAliasToCanonicalProvider() = runTest {
        val source = FakeLocationsDataSource(
            stored = LocationBundleV4(
                activeLocationId = "wb",
                locations = listOf(
                    LocationEntry(
                        storageId = "wb",
                        name = "WB",
                        legacyId = "room-wb",
                        legacyKey = "key-wb",
                        legacyBypassProvider = "wbstream"
                    )
                )
            )
        )

        val active = LocationsRepositoryImpl(source).getActiveLocation()

        assertNotNull(active)
        assertEquals(LocationConfig.PROVIDER_WB_STREAM, active.bypassProvider)
        assertEquals(LocationConfig.PROVIDER_WB_STREAM, active.location.bypassProvider)
    }

    @Test
    fun importsWbStreamAliasAsCanonicalProvider() = runTest {
        val source = FakeLocationsDataSource()
        val input = """
            {
              "version": 3,
              "active_location_id": "wb",
              "locations": [
                {
                  "storage_id": "wb",
                  "name": "WB",
                  "id": "room-wb",
                  "key": "key-wb",
                  "bypass_provider": "wbstream"
                }
              ]
            }
        """.trimIndent()

        LocationsRepositoryImpl(source).importText(input)

        val imported = source.stored
        assertNotNull(imported)
        assertEquals(LocationConfig.PROVIDER_WB_STREAM, imported.locations.first().bypassProvider)
    }

    @Test
    fun importsSingleLegacyLocationWithTurnProvider() = runTest {
        val source = FakeLocationsDataSource()
        val input = """
            {
              "hysteria": {
                "name": "Paris",
                "server": "room-paris",
                "password": "key-paris"
              },
              "turn": {
                "type": "telemost"
              }
            }
        """.trimIndent()

        LocationsRepositoryImpl(source).importText(input)

        val imported = source.stored
        assertNotNull(imported)
        assertEquals("imported_paris", imported.activeLocationId)
        assertEquals(1, imported.locations.size)
        assertEquals("room-paris", imported.locations.first().location.id)
        assertEquals(
            LocationConfig.PROVIDER_TELEMOST,
            imported.locations.first().location.bypassProvider
        )
    }

    @Test
    fun importsOlcRtcUriWithClientIdAndMimoName() = runTest {
        val source = FakeLocationsDataSource()
        val input = "olcrtc://wbstream?seichannel@room-01#${"a".repeat(64)}%android-01${'$'}RU / olc free sub / IPv6"

        LocationsRepositoryImpl(source).importText(input)

        val imported = source.stored
        assertNotNull(imported)
        val entry = imported.locations.single()
        val location = entry.location
        assertEquals(LocationConfig.PROVIDER_WB_STREAM, location.bypassProvider)
        assertEquals(LocationConfig.TRANSPORT_SEICHANNEL, location.transport)
        assertEquals("room-01", location.id)
        assertEquals("android-01", location.clientId)
        assertEquals("RU / olc free sub / IPv6", location.name)
        assertEquals("RU / olc free sub / IPv6", entry.metadata?.mimo)
        assertNull(entry.metadata?.subscription)
    }

    @Test
    fun importsOlcRtcSubscriptionAndAppliesLocalNames() = runTest {
        val source = FakeLocationsDataSource()
        val input = """
            #name: Test subscription
            #update: 1778011200
            #refresh: 10m
            #color: #4A90E2
            #icon: flag-ru
            #used: 10mb/10gb
            #available: 9.99gb

            olcrtc://wbstream?seichannel@room-01#${"a".repeat(64)}%android-01${'$'}RU / default name
            ##name: RU-1
            ##color: #4A90E2
            ##icon: node-ru
            ##used: 500mb/10gb
            ##available: 9.5gb
            ##ip: 203.0.113.10
            ##comment: primary

            olcrtc://jazz?datachannel@room-02#${"b".repeat(64)}%android-02${'$'}DE / backup
            ##name: DE-Backup
        """.trimIndent()

        LocationsRepositoryImpl(source).importText(input)

        val imported = source.stored
        assertNotNull(imported)
        assertEquals(listOf("RU-1", "DE-Backup"), imported.locations.map { it.location.name })
        assertEquals(listOf("android-01", "android-02"), imported.locations.map { it.location.clientId })
        assertEquals("imported_ru-1", imported.activeLocationId)

        val firstMetadata = imported.locations[0].metadata
        assertNotNull(firstMetadata)
        assertEquals("RU-1", firstMetadata.name)
        assertEquals("#4A90E2", firstMetadata.color)
        assertEquals("node-ru", firstMetadata.icon)
        assertEquals("500mb/10gb", firstMetadata.used)
        assertEquals("9.5gb", firstMetadata.available)
        assertEquals("203.0.113.10", firstMetadata.ip)
        assertEquals("primary", firstMetadata.comment)
        assertEquals("RU / default name", firstMetadata.mimo)

        val subscriptionMetadata = firstMetadata.subscription
        assertNotNull(subscriptionMetadata)
        assertEquals("Test subscription", subscriptionMetadata.name)
        assertEquals("1778011200", subscriptionMetadata.update)
        assertEquals("10m", subscriptionMetadata.refresh)
        assertEquals("#4A90E2", subscriptionMetadata.color)
        assertEquals("flag-ru", subscriptionMetadata.icon)
        assertEquals("10mb/10gb", subscriptionMetadata.used)
        assertEquals("9.99gb", subscriptionMetadata.available)
        assertEquals(subscriptionMetadata, imported.locations[1].metadata?.subscription)
    }

    @Test
    fun rejectsHttpSubscriptionImportsAndKeepsExistingLocationsUntouched() = runTest {
        val existing = LocationEntry.from(
            "existing",
            LocationConfig("Existing", "room-existing", "k".repeat(64), LocationConfig.PROVIDER_JAZZ)
        )
        val source = FakeLocationsDataSource(
            stored = LocationBundleV4(
                activeLocationId = "existing",
                locations = listOf(existing)
            )
        )

        LocationsRepositoryImpl(source).importText("http://example.com/sub.txt")

        val stored = source.stored
        assertNotNull(stored)
        assertEquals(listOf("existing"), stored.locations.map { it.storageId })
        assertEquals("existing", stored.activeLocationId)
    }

    @Test
    fun importsHttpsSubscriptionAndStoresSourceUrl() = runTest {
        val source = FakeLocationsDataSource()
        val client = HttpClient(MockEngine { request ->
            assertEquals("https://example.com/sub.txt", request.url.toString())
            respond(
                content = "olcrtc://wbstream?datachannel@room-01#${"a".repeat(64)}%android-01${'$'}RU / secure",
                status = HttpStatusCode.OK,
                headers = headersOf("Content-Type", ContentType.Text.Plain.toString())
            )
        })

        LocationsRepositoryImpl(source, client).importText("https://example.com/sub.txt")

        val stored = source.stored
        assertNotNull(stored)
        assertEquals("https://example.com/sub.txt", stored.locations.single().subscriptionUrl)
        assertEquals("room-01", stored.locations.single().location.id)
    }

    @Test
    fun importUpdatesMatchingStorageIdsAndAppendsNewLocations() = runTest {
        val source = FakeLocationsDataSource(
            stored = LocationBundleV4(
                activeLocationId = "custom_paris",
                locations = listOf(
                    LocationEntry.from(
                        "custom_paris",
                        LocationConfig(
                            name = "Paris",
                            id = "room-old",
                            key = "a".repeat(64),
                            bypassProvider = LocationConfig.PROVIDER_WB_STREAM
                        )
                    ),
                    LocationEntry.from(
                        "custom_berlin",
                        LocationConfig(
                            name = "Berlin",
                            id = "room-berlin",
                            key = "b".repeat(64),
                            bypassProvider = LocationConfig.PROVIDER_TELEMOST
                        )
                    )
                )
            )
        )
        val input = """
            {
              "version": 4,
              "active_location_id": "sub_wb",
              "locations": [
                {
                  "storage_id": "custom_paris",
                  "name": "Paris updated",
                  "endpoint": {
                    "room_id": "room-new",
                    "key": "${"c".repeat(64)}",
                    "client_id": "phone-1"
                  },
                  "carrier": "wbstream",
                  "transport": {
                    "type": "datachannel"
                  }
                },
                {
                  "storage_id": "sub_wb",
                  "name": "WB sub",
                  "subscription_url": "https://example.com/sub.md",
                  "endpoint": {
                    "room_id": "room-sub",
                    "key": "${"d".repeat(64)}",
                    "client_id": "phone-2"
                  },
                  "carrier": "wbstream",
                  "transport": {
                    "type": "vp8channel"
                  }
                }
              ]
            }
        """.trimIndent()

        LocationsRepositoryImpl(source).importText(input)

        val imported = source.stored
        assertNotNull(imported)
        assertEquals(listOf("custom_paris", "custom_berlin", "sub_wb"), imported.locations.map { it.storageId })
        assertEquals("room-new", imported.locations[0].location.id)
        assertEquals("room-berlin", imported.locations[1].location.id)
        assertEquals("https://example.com/sub.md", imported.locations[2].subscriptionUrl)
        assertEquals("sub_wb", imported.activeLocationId)
    }

    @Test
    fun invalidLocationCannotBecomeActiveLocation() = runTest {
        val source = FakeLocationsDataSource()
        val incomplete = LocationConfig(name = "Broken", id = "room", key = "")

        LocationsRepositoryImpl(source).saveLocation("broken", incomplete)

        val bundle = source.stored
        assertNotNull(bundle)
        assertNull(bundle.activeLocationId)
        assertTrue(bundle.locations.isEmpty())
    }

    @Test
    fun telemostLocationsForceVp8AndOtherProvidersCanUseDatachannel() = runTest {
        val source = FakeLocationsDataSource()
        val input = """
            {
              "version": 3,
              "locations": [
                {
                  "storage_id": "telemost",
                  "name": "Telemost",
                  "id": "75047680642749",
                  "key": "${"a".repeat(64)}",
                  "bypass_provider": "telemost",
                  "transport": "datachannel"
                },
                {
                  "storage_id": "wb",
                  "name": "WB",
                  "id": "room-wb",
                  "key": "${"b".repeat(64)}",
                  "bypass_provider": "wbstream",
                  "transport": "datachannel"
                }
              ]
            }
        """.trimIndent()

        LocationsRepositoryImpl(source).importText(input)

        val imported = source.stored
        assertNotNull(imported)
        assertEquals(LocationConfig.TRANSPORT_VP8CHANNEL, imported.locations[0].location.transport)
        assertEquals(LocationConfig.TRANSPORT_DATACHANNEL, imported.locations[1].location.transport)
    }

    @Test
    fun importsUnsupportedVideochannelAsDefaultTransport() = runTest {
        val source = FakeLocationsDataSource()
        val input = """
            {
              "version": 4,
              "active_location_id": "telemost-video",
              "locations": [
                {
                  "storage_id": "telemost-video",
                  "name": "Telemost Video",
                  "endpoint": {
                    "room_id": "75047680642749",
                    "key": "${"c".repeat(64)}"
                  },
                  "carrier": "telemost",
                  "transport": {
                    "type": "videochannel"
                  }
                }
              ]
            }
        """.trimIndent()

        LocationsRepositoryImpl(source).importText(input)

        val imported = source.stored
        assertNotNull(imported)
        val location = imported.locations.first().location
        assertEquals(4, imported.version)
        assertEquals(LocationConfig.PROVIDER_TELEMOST, location.bypassProvider)
        assertEquals(LocationConfig.TRANSPORT_VP8CHANNEL, location.transport)
    }

    @Test
    fun exposesAllWorkingCarrierTransportPairs() {
        assertEquals(
            listOf(
                LocationConfig.TRANSPORT_VP8CHANNEL,
                LocationConfig.TRANSPORT_SEICHANNEL
            ),
            LocationConfig.supportedTransportsForProvider(LocationConfig.PROVIDER_TELEMOST)
        )
        assertEquals(
            listOf(
                LocationConfig.TRANSPORT_DATACHANNEL,
                LocationConfig.TRANSPORT_VP8CHANNEL,
                LocationConfig.TRANSPORT_SEICHANNEL
            ),
            LocationConfig.supportedTransportsForProvider(LocationConfig.PROVIDER_JAZZ)
        )
        assertEquals(
            LocationConfig.supportedTransportsForProvider(LocationConfig.PROVIDER_JAZZ),
            LocationConfig.supportedTransportsForProvider(LocationConfig.PROVIDER_WB_STREAM)
        )
    }

    private class FakeLocationsDataSource(
        var stored: LocationBundleV4? = null,
        private val legacy: List<Pair<String, String>> = emptyList(),
        private val legacyActive: String? = null
    ) : LocationsDataSource {

        override suspend fun loadLocationBundle(): LocationBundleV4? = stored

        override suspend fun saveLocationBundle(bundle: LocationBundleV4) {
            stored = bundle
        }

        override suspend fun loadLegacyLocations(): List<Pair<String, String>> = legacy

        override suspend fun loadLegacyActiveLocationId(): String? = legacyActive
    }
}
