---
name: koredb-migration-room
description: Migrate Android applications from Room / SQLite to KoreDB. Use when converting Room @Entity, @Dao, @Database, @Query, or @TypeConverter classes into KoreDB document collections, implementing one-time background migration workers, replacing SQLite cursor windows, or converting SQL queries to KoreDB Query DSL.
---

# Room / SQLite to KoreDB Migration Guide

This skill guides AI agents and developers in systematically migrating Android codebases from **Android Room (SQLite)** to **KoreDB**.

---

## 1. Conceptual Translation Table

| Room / SQLite Construct | KoreDB Equivalent | Notes |
| :--- | :--- | :--- |
| `@Entity(tableName = "items")` | `@Serializable data class Item(...)` | No annotations required. Pure Kotlin data class. |
| `@PrimaryKey val id: String` | `val id: String` + `idExtractor = { it.id }` | Passed when declaring `db.collection("items", ...) { it.id }`. |
| `@ColumnInfo(name = "user_name")` | Normal property: `val userName: String` | Encoded directly into CBOR binary format. |
| `@Index(value = ["category"])` | `collection.createIndex("category") { it.category }` | Dual-write LSM secondary index. |
| `@Index(value = ["price"])` | `collection.createNumericIndex("price") { it.price }` | Order-preserving binary range pushdown. |
| `@TypeConverter` | Built-in Kotlinx Serialization | Enums, Lists, Nested Maps, and Objects serialize automatically. |
| `@Dao` Interface | `KoreCollection<T>` Methods & Query DSL | Direct collection API with full CRUD and Flow support. |
| `@Query("SELECT * FROM ...")` | `collection.query { where(...) }` | Strongly typed DSL without runtime SQL parsing. |
| `@Transaction` | `db.transaction { ... }` | ACID MVCC Snapshot Isolation. |

---

## 2. Converting DAOs to KoreDB Collections

### Example: E-Commerce Product DAO

#### Before (Room):
```kotlin
@Dao
interface ProductDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insert(product: ProductEntity)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAll(products: List<ProductEntity>)

    @Query("SELECT * FROM products WHERE id = :id")
    suspend fun getById(id: String): ProductEntity?

    @Query("SELECT * FROM products WHERE category = :category")
    suspend fun getByCategory(category: String): List<ProductEntity>

    @Query("SELECT * FROM products WHERE price BETWEEN :min AND :max ORDER BY price ASC")
    suspend fun getByPriceRange(min: Double, max: Double): List<ProductEntity>

    @Query("DELETE FROM products WHERE id = :id")
    suspend fun deleteById(id: String)

    @Query("SELECT * FROM products")
    fun observeAll(): Flow<List<ProductEntity>>
}
```

#### After (KoreDB Repository Implementation):
```kotlin
class ProductRepository(private val db: KoreDatabase) {

    private val products = db.collection("products", Product.serializer()) { it.id }

    init {
        products.createIndex("category") { it.category }
        products.createNumericIndex("price") { it.price }
    }

    suspend fun insert(product: Product) = products.insert(product.id, product)

    suspend fun insertAll(list: List<Product>) = products.insertAll(list)

    suspend fun getById(id: String): Product? = products.getById(id)

    suspend fun getByCategory(category: String): List<Product> =
        products.find("category", category)

    suspend fun getByPriceRange(min: Double, max: Double): List<Product> =
        products.query {
            where("price", between(min, max))
            sortBy("price", ascending = true)
        }

    suspend fun deleteById(id: String) = products.delete(id)

    fun observeAll(): Flow<List<Product>> = products.observe().map { products.getAll() }
}
```

---

## 3. SQL Query to KoreDB DSL Conversion Reference

### Equality & Negation
* **SQL**: `WHERE status = 'active'`  
  **KoreDB**: `where("status", eq("active"))`
* **SQL**: `WHERE status != 'archived'`  
  **KoreDB**: `where("status", neq("archived"))`

### Numeric & Range
* **SQL**: `WHERE age >= 18 AND age <= 65`  
  **KoreDB**: `where("age", between(18.0, 65.0))` or `where("age", gte(18.0)).where("age", lte(65.0))`

### IN Clauses
* **SQL**: `WHERE category IN ('books', 'music')`  
  **KoreDB**: `where("category", inList(listOf("books", "music")))`

### Substring / LIKE
* **SQL**: `WHERE title LIKE '%kotlin%'`  
  **KoreDB**: `where("title", contains("kotlin"))`  
  *(For production search, prefer BM25: `products.search("kotlin", limit = 20)`)*

### Pagination & Ordering
* **SQL**: `ORDER BY price DESC LIMIT 20 OFFSET 40`  
  **KoreDB**: `sortBy("price", ascending = false).limit(20).offset(40)`

---

## 4. Production Data Migration Worker (Zero Downtime)

When migrating an existing app installed on user devices, read from Room and batch-insert into KoreDB on first launch:

```kotlin
import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class RoomToKoreMigrationWorker(
    appContext: Context,
    workerParams: WorkerParameters,
    private val roomDb: AppDatabase,
    private val koreDb: KoreDatabase
) : CoroutineWorker(appContext, workerParams) {

    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        val prefs = applicationContext.getSharedPreferences("migration_prefs", Context.MODE_PRIVATE)
        if (prefs.getBoolean("koredb_migration_complete", false)) {
            return@withContext Result.success()
        }

        val targetCollection = koreDb.collection("products", Product.serializer()) { it.id }

        // Read from Room in bounded 1,000-item chunks to avoid Android CursorWindow 2MB OOM
        var offset = 0
        val batchSize = 1000

        while (true) {
            val chunk = roomDb.productDao().getPage(limit = batchSize, offset = offset)
            if (chunk.isEmpty()) break

            val converted = chunk.map { entity ->
                Product(
                    id = entity.id,
                    title = entity.title,
                    category = entity.category,
                    price = entity.price
                )
            }
            targetCollection.insertAll(converted)
            offset += chunk.size
        }

        // Mark complete and safely delete old SQLite database files
        prefs.edit().putBoolean("koredb_migration_complete", true).apply()
        roomDb.close()
        applicationContext.deleteDatabase("legacy_room.db")

        Result.success()
    }
}
```
