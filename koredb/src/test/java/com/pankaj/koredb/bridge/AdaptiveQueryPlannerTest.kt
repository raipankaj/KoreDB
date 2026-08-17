/*
 * Copyright 2026 KoreDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pankaj.koredb.bridge

import com.pankaj.koredb.core.KoreVectorCollection
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.graph.Node
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

class AdaptiveQueryPlannerTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase
    private lateinit var graph: GraphStorage
    private lateinit var vectors: KoreVectorCollection
    private lateinit var bridge: GraphVectorBridge

    @Before
    fun setup() {
        testDir = File("build/tmp/test_adaptive_planner_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
        graph = db.graph()
        vectors = db.vectorCollection("items")
        bridge = db.graphVectorBridge(vectors)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun testAdaptiveOverfetchingRecoversRequestedLimit() = runBlocking {
        // Insert 40 items. Only multiples of 4 (item_0, item_4, item_8, ...) are "Verified" (25% selectivity)
        val nodes = mutableListOf<Node>()
        for (i in 0 until 40) {
            val id = "item_$i"
            val vec = floatArrayOf(1.0f, (i % 5) * 0.1f, (i % 3) * 0.1f)
            vectors.insert(id, vec)
            val isVerified = (i % 4 == 0)
            nodes.add(Node(id = id, labels = setOf("Product"), properties = mapOf("verified" to isVerified.toString())))
        }
        graph.putNodes(nodes)
        vectors.waitForIndexing()

        val query = floatArrayOf(1.0f, 0.0f, 0.0f)
        val targetLimit = 5

        // 1. Static search with fixed limit=5 only gets 1 or 2 matching items
        val staticResults = bridge.vectorSearch(query, limit = targetLimit)
            .filterByGraph { id ->
                val node = graph.getNode(id)
                node?.properties?.get("verified") == "true"
            }
        assertTrue("Static search underfetches due to selective predicate (got ${staticResults.size})", staticResults.size < targetLimit)

        // 2. Adaptive over-fetching expands k dynamically until targetLimit=5 is satisfied
        val adaptiveResults = bridge.adaptiveVectorSearch(
            query = query,
            targetLimit = targetLimit,
            predicateTag = "verified_products",
            maxK = 40
        ) { id ->
            val node = graph.getNode(id)
            node?.properties?.get("verified") == "true"
        }

        assertEquals("Adaptive search should satisfy targetLimit", targetLimit, adaptiveResults.size)
        adaptiveResults.forEach { result ->
            assertEquals("true", result.node?.properties?.get("verified"))
        }
    }

    @Test
    fun testPlannerSelectsGraphFirstWhenCandidatesSmall() {
        val planner = HybridQueryPlanner()
        // 5 candidates vs targetLimit 10 -> Graph-First strictly optimal
        val (strategy, costs) = planner.chooseStrategy(
            targetLimit = 10,
            candidateNodeCount = 5,
            predicateTag = "friend_filter"
        )
        assertEquals(QueryStrategy.GRAPH_FIRST, strategy)
        assertTrue("Graph-First cost should be lower than Vector-First cost", costs.first < costs.second)
    }

    @Test
    fun testPlannerSelectsVectorFirstWhenCandidatesLargeOrUnspecified() {
        val planner = HybridQueryPlanner()
        // 10,000 candidates vs targetLimit 10 -> Vector-First with adaptive over-fetching
        val (strategy, costs) = planner.chooseStrategy(
            targetLimit = 10,
            candidateNodeCount = 10000,
            predicateTag = "all_items"
        )
        assertEquals(QueryStrategy.VECTOR_FIRST_ADAPTIVE, strategy)
        assertTrue("Vector-First cost should be lower than scoring 10,000 graph candidates", costs.second < costs.first)
    }

    @Test
    fun testSelectivityStatsTrackerEMA() {
        val tracker = QueryStatsTracker(smoothingFactor = 0.5f)
        assertEquals(0.5f, tracker.estimateSelectivity("custom_tag"), 0.001f)

        // Record a low selectivity pass (10 / 100 = 0.10)
        tracker.recordExecution("custom_tag", 100, 10)
        val updatedSelectivity = tracker.estimateSelectivity("custom_tag")
        assertTrue("Selectivity should adapt downwards", updatedSelectivity < 0.5f)

        // Recommended initial K should automatically scale up
        val initialK = tracker.calculateInitialK(targetLimit = 10, predicateTag = "custom_tag", maxK = 500)
        assertTrue("Initial K should scale up for low selectivity", initialK >= 30)
    }

    @Test
    fun testExplainPlanFormatting() = runBlocking {
        val nodes = mutableListOf<Node>()
        for (i in 0 until 10) {
            vectors.insert("doc_$i", floatArrayOf(0.1f * i, 0.2f, 0.3f))
            nodes.add(Node(id = "doc_$i", labels = setOf("Doc")))
        }
        graph.putNodes(nodes)
        vectors.waitForIndexing()

        val plan = bridge.explain(
            query = floatArrayOf(0.1f, 0.2f, 0.3f),
            targetLimit = 3,
            predicateTag = "test_tag",
            graphPredicate = { true }
        )

        assertNotNull(plan)
        assertEquals(QueryStrategy.VECTOR_FIRST_ADAPTIVE, plan.strategy)
        assertEquals(3, plan.resultCount)
        assertTrue(plan.actualExecutionTimeMs >= 0)

        val explainOutput = plan.explainString()
        assertTrue(explainOutput.contains("KoreDB Hybrid Query Execution Plan"))
        assertTrue(explainOutput.contains("Strategy: VECTOR_FIRST_ADAPTIVE"))
        assertTrue(explainOutput.contains("Final Results:     3"))
    }
}
