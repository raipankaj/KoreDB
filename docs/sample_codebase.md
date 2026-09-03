# Complete Sample Codebase: AI E-Commerce & Recommendations

This guide provides a production-ready, end-to-end Kotlin implementation demonstrating how to integrate KoreDB's **Document Collections**, **HNSW Vector Search**, and **Property Graph** into an Android application.

---

## 1. Data Models (`Models.kt`)

```kotlin
package com.example.ecommerce.model

import kotlinx.serialization.Serializable

@Serializable
data class Product(
    val id: String,
    val title: String,
    val category: String,
    val price: Double,
    val tags: List<String>
)

@Serializable
data class UserProfile(
    val id: String,
    val username: String,
    val email: String,
    val preferredCategories: List<String>
)
```

---

## 2. Database Module (`DatabaseModule.kt`)

```kotlin
package com.example.ecommerce.di

import android.content.Context
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.hnsw.DistanceMetric
import java.io.File

object DatabaseModule {

    @Volatile
    private var instance: KoreDatabase? = null

    fun provideDatabase(context: Context): KoreDatabase {
        return instance ?: synchronized(this) {
            instance ?: KoreDatabase(
                directory = File(context.filesDir, "koredb_store"),
                enableCdc = true // Change Data Capture for cloud sync
            ).also { db ->
                configureIndices(db)
                instance = db
            }
        }
    }

    private fun configureIndices(db: KoreDatabase) {
        // 1. Configure Document Collection Indices
        val products = db.binaryCollection<com.example.ecommerce.model.Product>("products")
        products.createIndex("category") { it.category }
        products.createNumericIndex("price") { it.price }
        products.createSearchableIndex("title") { it.title }

        // 2. Configure Vector Collection
        db.vectorCollection("product_embeddings") {
            dimensions = 128
            metric = DistanceMetric.COSINE
            quantization = true // 4x memory savings with SQ8
        }
    }
}
```

---

## 3. Repository Layer (`ProductRepository.kt`)

```kotlin
package com.example.ecommerce.data

import com.example.ecommerce.model.Product
import com.pankaj.koredb.bridge.GraphVectorBridge
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class ProductRepository(private val db: KoreDatabase) {

    private val products = db.binaryCollection<Product>("products")
    private val vectors = db.vectorCollection("product_embeddings") { dimensions = 128 }
    private val graph = db.graph()
    private val bridge = GraphVectorBridge(db)

    // --- DOCUMENT OPERATIONS ---

    suspend fun saveProduct(product: Product, embedding: FloatArray) = withContext(Dispatchers.IO) {
        // 1. Save Document
        products.insert(product.id, product)

        // 2. Index Vector Embedding
        vectors.insert(
            id = product.id,
            vector = embedding,
            metadata = mapOf("category" to product.category, "price" to product.price)
        )

        // 3. Register in Graph
        graph.putNode(Node(
            id = product.id,
            labels = setOf("Product"),
            properties = mapOf("title" to product.title, "category" to product.category)
        ))
    }

    fun findProductsByCategory(category: String): List<Product> {
        return products.find("category", category)
    }

    fun findProductsByPriceRange(minPrice: Double, maxPrice: Double): List<Product> {
        return products.findRange("price", minPrice, maxPrice)
    }

    // --- GRAPH RELATIONSHIP OPERATIONS ---

    suspend fun recordPurchase(userId: String, productId: String) = withContext(Dispatchers.IO) {
        db.transaction { tx ->
            val g = db.graph()
            g.putEdge(Edge(
                sourceId = userId,
                targetId = productId,
                type = "PURCHASED",
                weight = 1.0,
                properties = mapOf("timestamp" to System.currentTimeMillis().toString())
            ))
        }
    }

    // --- AI SEMANTIC SEARCH & HYBRID RECOMMENDATIONS ---

    suspend fun semanticSearch(queryVector: FloatArray, maxPrice: Double): List<Product> {
        val matches = vectors.search(queryVector, limit = 10) {
            where("price", lte(maxPrice))
        }
        return matches.mapNotNull { (id, _) -> products.getById(id) }
    }

    suspend fun getSocialRecommendations(userId: String, queryVector: FloatArray): List<Product> {
        // Hybrid Graph RAG:
        // Traverse: (User) -> FOLLOWS -> (Friends) -> PURCHASED -> (Products)
        // Rerank by semantic similarity to user's intent vector
        val candidateResults = bridge.graphFirst(userId)
            .traverse(edgeType = "FOLLOWS", maxDepth = 2)
            .traverse(edgeType = "PURCHASED", maxDepth = 1)
            .rerankByVector(
                vectorCollectionName = "product_embeddings",
                queryVector = queryVector,
                limit = 10
            )

        return candidateResults.mapNotNull { result -> products.getById(result.id) }
    }
}
```

---

## 4. ViewModel Layer (`ProductViewModel.kt`)

```kotlin
package com.example.ecommerce.ui

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.example.ecommerce.data.ProductRepository
import com.example.ecommerce.model.Product
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

class ProductViewModel(private val repository: ProductRepository) : ViewModel() {

    private val _items = MutableStateFlow<List<Product>>(emptyList())
    val items: StateFlow<List<Product>> = _items.asStateFlow()

    fun filterByCategory(category: String) {
        viewModelScope.launch {
            _items.value = repository.findProductsByCategory(category)
        }
    }

    fun searchSemantically(queryEmbedding: FloatArray, maxBudget: Double) {
        viewModelScope.launch {
            _items.value = repository.semanticSearch(queryEmbedding, maxBudget)
        }
    }

    fun loadFriendRecommendations(userId: String, intentEmbedding: FloatArray) {
        viewModelScope.launch {
            _items.value = repository.getSocialRecommendations(userId, intentEmbedding)
        }
    }
}
```
