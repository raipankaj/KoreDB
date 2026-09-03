# Enterprise Capabilities & Security

KoreDB is designed for enterprise-grade mobile deployments, offering production capabilities for security, compliance, data replication, and space efficiency.

---

## 1. Change Data Capture (CDC) & Replication

Change Data Capture (CDC) provides an append-only, ordered event stream of every database mutation (INSERT, UPDATE, DELETE) across all collections.

### Enabling CDC
CDC is enabled by default when initializing `KoreDatabase`:

```kotlin
val db = KoreDatabase(dataDir, enableCdc = true)
val cdc = db.cdcManager
```

### Subscribing to Real-Time Mutation Streams
```kotlin
// Observe all mutations across the entire database
CoroutineScope(Dispatchers.Default).launch {
    cdc.changes.collect { record ->
        println("[CDC EVENT #${record.sequenceNumber}] " +
                "Collection: ${record.collection}, " +
                "Key: ${record.documentId}, " +
                "Op: ${record.op}, " +
                "Timestamp: ${record.timestamp}")

        // Stream event to remote sync server / Kafka / Supabase
        syncEngine.pushToServer(record)
    }
}
```

### Querying Historical Changes
```kotlin
// Replay changes since sequence number 500 for incremental offline sync
val missedChanges = cdc.getChangesSince(sequenceNumber = 500)
```

---

## 2. Hardware-Accelerated Encryption (AES-GCM-256 with AAD)

Protect sensitive on-device data with authenticated encryption:

- **Algorithm**: `AES-256-GCM` (Galois/Counter Mode) with 12-byte random IVs and 128-bit authentication tags.
- **Associated Authenticated Data (AAD)**: The document key is passed as AAD during encryption. This prevents ciphertext relocation attacks (i.e. moving an encrypted payload to a different key).
- **Zero-Copy In-Place Offsets**: Cipher transformations eliminate intermediate array allocations.

```kotlin
import com.pankaj.koredb.crypto.KoreCrypto

val keyBytes = get256BitMasterKeyFromAndroidKeystore()
val crypto = KoreCrypto(keyBytes)

val db = KoreDatabase(dataDir, crypto = crypto)
```

---

## 3. High-Throughput Compression Codecs

KoreDB supports pluggable compression codecs on SSTable disk blocks:

1. **`Lz4CompressionCodec` (Default)**: Ultra-fast decompression (~3–4 GB/s) with 2x–3x compression ratio. Includes small-payload bypass (< 5 bytes) and compressibility fallbacks to avoid data expansion.
2. **`ZstdCompressionCodec`**: High-ratio compression for cold archives.
3. **`NoOpCompressionCodec`**: Zero CPU overhead for already-compressed payloads (e.g. pre-compressed images or audio).

---

## 4. Crash Recovery & Tail Corruption Discard

Mobile applications frequently experience process death, battery exhaustion, or forced app termination mid-transaction.

### Automatic Recovery Protocol:
1. When opened, KoreDB parses the WAL from byte offset 0.
2. If an unfinished transaction tail is encountered (e.g., power loss before writing `RECORD_COMMIT`), the CRC check fails.
3. KoreDB discards the corrupt tail and truncates the WAL to the last confirmed commit boundary (`truncateCorruptTail`), preventing database corruption.

---

## 5. Leveled Compaction & Maintenance

Trigger on-demand background compaction during device idle / charging states to reclaim disk space:

```kotlin
// Trigger leveled compaction and disk sync
db.compact()
```
