# Migrating from Android Room / SQLite to KoreDB

This guide walks through migrating an Android application from **Room / SQLite** to **KoreDB**, providing code mappings, architectural comparisons, and zero-downtime migration strategies.

---

## 1. Architectural Differences at a Glance

| Feature | Android Room (SQLite) | KoreDB |
| :--- | :--- | :--- |
| **Storage Architecture** | B-Tree page-based table file (`.db`) | **LSM-Tree** (MemTable + WAL + tiered SSTables) |
| **Data Format** | Relational rows & SQL columns | **Typed CBOR binary documents**, Vectors, and Graphs |
| **Schema Definition** | `@Entity`, `@ColumnInfo`, SQL tables | Kotlin `@Serializable data class` |
| **Primary Keys** | `@PrimaryKey` (AutoGenerate or manual) | String IDs (`idExtractor: (T) -> String`) |
| **Indices** | SQL B-Tree index (`@Index`) | Secondary string index & order-preserving numeric index |
| **Vector Similarity** | ❌ None (Requires SQLite-VSS or external library) | **Native HNSW, SQ8 Quantization, Off-Heap Mmap** |
| **Graph Traversal** | Recursive SQL CTEs (`WITH RECURSIVE`) | **Native Bidirectional Property Graph (BFS, Dijkstra, PageRank)** |
| **Concurrency** | Read-pool + Single-writer transaction | **Direct commit pipeline + Concurrent readers + MVCC** |
| **Memory Footprint** | SQLite C-heap + Android cursor window | **JVM off-heap mmap + DirectByteBuffer + BlockCache** |

---

## 2. Concept & Annotation Mapping

### Schema Definition

#### In Room:
```kotlin
@Entity(
    tableName = "products",
    indices = [
        Index(value = ["category"]),
        Index(value = ["price"])
    ]
)
data class ProductEntity(
    @PrimaryKey val id: String,
    @ColumnInfo(name = "title") val title: String,
    @ColumnInfo(name = "category") val category: String,
    @ColumnInfo(name = "price") val price: Double,
    @ColumnInfo(name = "stock") val stock: Int
)
```

#### In KoreDB:
```kotlin
import kotlinx.serialization.Serializable

@Serializable
data class Product(
    val id: String,
    val title: String,
    val category: String,
    val price: Double,
    val stock: Int
)
```

> In KoreDB, no boilerplate annotations like `@Entity` or `@ColumnInfo` are required. Simply declare your model as standard `@Serializable`. Serialization defaults to high-speed binary **CBOR**, avoiding SQL type-casting and cursor window serialization overhead.

---

### Database Initialization & DI

#### In Room:
```kotlin
@Database(entities = [ProductEntity::class], version = 1)
abstract class AppDatabase : RoomDatabase() {
    abstract fun productDao(): ProductDao
}

val roomDb = Room.databaseBuilder(context, AppDatabase::class.java, "shop.db")
    .fallbackToDestructiveMigration(true)
    .build()
```

#### In KoreDB:
```kotlin
val koreDb = KoreAndroid.builder(context, "shop.db")
    .withCompression(Lz4CompressionCodec()) // Optional LZ4 compression
    .minFreeSpaceMb(20)                     // Disk safety threshold
    .schemaVersion(1) { db, oldVersion, newVersion ->
        // Custom migration logic
    }
    .build()

// Access collection
val products = koreDb.collection("products", Product.serializer()) { it.id }
products.createIndex("category") { it.category }
products.createNumericIndex("price") { it.price }
```

---

### CRUD & Query Translation

#### 1. Insert & Batch Insert
```kotlin
// Room
productDao.insert(product)
productDao.insertAll(productList)

// KoreDB
products.insert(product.id, product)
products.insertAll(productList) // 1.5x - 25x faster than Room
```

#### 2. Point Read by ID
```kotlin
// Room DAO
@Query("SELECT * FROM products WHERE id = :id")
suspend fun getById(id: String): ProductEntity?

// KoreDB
val product: Product? = products.getById("p100") // Zero-copy fast read (6ms for 10k reads)
```

#### 3. Filter & Range Queries
```kotlin
// Room DAO
@Query("SELECT * FROM products WHERE category = :category AND price <= :maxPrice")
suspend fun findByCategoryAndMaxPrice(category: String, maxPrice: Double): List<ProductEntity>

// KoreDB Query DSL
val items = products.query {
    where("category", eq("electronics"))
    where("price", lte(500.0))
}
```

#### 4. Reactive UI Observation (Flow)
```kotlin
// Room DAO
@Query("SELECT * FROM products")
fun observeAll(): Flow<List<ProductEntity>>

// KoreDB
val updatesFlow: Flow<String> = products.observe() // Emits updated record IDs or "*"
```

---

## 3. Step-by-Step Data Migration Strategy

To seamlessly migrate users with existing Room SQLite data to KoreDB without data loss:

### Option A: One-Time Startup Migration (Recommended)

Execute during app initialization (e.g. inside `Application.onCreate` or a splash-screen startup worker):

```kotlin
class DatabaseMigrator(
    private val context: Context,
    private val roomDb: AppDatabase,
    private val koreDb: KoreDatabase
) {
    suspend fun migrateIfNeeded() = withContext(Dispatchers.IO) {
        val prefs = context.getSharedPreferences("db_migration", Context.MODE_PRIVATE)
        val isMigrated = prefs.getBoolean("room_to_kore_migrated", false)
        if (isMigrated) return@withContext

        val productCollection = koreDb.collection("products", Product.serializer()) { it.id }

        // Read in batches from Room SQLite to prevent memory pressure
        var offset = 0
        val batchSize = 1000
        while (true) {
            val roomBatch = roomDb.productDao().getPage(limit = batchSize, offset = offset)
            if (roomBatch.isEmpty()) break

            val koreBatch = roomBatch.map { entity ->
                Product(
                    id = entity.id,
                    title = entity.title,
                    category = entity.category,
                    price = entity.price,
                    stock = entity.stock
                )
            }
            productCollection.insertAll(koreBatch)
            offset += roomBatch.size
        }

        // Mark migration complete
        prefs.edit().putBoolean("room_to_kore_migrated", true).apply()

        // Optional: Close Room and purge old SQLite files to reclaim space
        roomDb.close()
        context.deleteDatabase("shop.db")
    }
}
```

---

## 4. Key Advantages Gained After Migration

1. **Massive Speedups**:
   - Up to **64x faster single reads** and **25x faster single writes**.
   - Up to **1,484x faster point queries** via MemTable and Mmap zero-copy SSTables.
2. **AI & Vector Embeddings**:
   - Instantly store embeddings (`FloatArray`) with native sub-millisecond HNSW KNN search without needing external libraries.
3. **Graph Traversals**:
   - Model complex user relationships, graph recommendations, and dependencies natively without multi-table SQL joins.
4. **Resilience & Zero Maintenance**:
   - No SQL schema locks, no SQLite `WAL_CHECKPOINT` hangs, and built-in CRC32 corruption self-healing.
