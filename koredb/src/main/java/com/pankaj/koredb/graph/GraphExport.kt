package com.pankaj.koredb.graph

/**
 * Exports graph data to standard interchange formats for visualization and analysis.
 *
 * Supported formats:
 * - **DOT** (Graphviz): For rendering with `dot`, `neato`, etc.
 * - **GraphML**: For import into Gephi, yEd, Cytoscape, and other graph tools.
 */
object GraphExport {

    /**
     * Exports a subgraph to DOT format (Graphviz).
     *
     * ```kotlin
     * val dot = GraphExport.toDot(storage, nodeIds, "KNOWS")
     * File("graph.dot").writeText(dot)
     * // Then: dot -Tpng graph.dot -o graph.png
     * ```
     *
     * @param storage The graph storage engine.
     * @param nodeIds The nodes to include in the export.
     * @param edgeType The relationship type to export (null = all types).
     * @param graphName Name of the graph.
     * @return A string in DOT format.
     */
    fun toDot(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeType: String? = null,
        graphName: String = "KoreDB"
    ): String {
        val sb = StringBuilder()
        sb.appendLine("digraph \"$graphName\" {")
        sb.appendLine("  rankdir=LR;")
        sb.appendLine("  node [shape=box, style=filled, fillcolor=\"#E8F4FD\", fontname=\"Inter\"];")
        sb.appendLine("  edge [fontname=\"Inter\", fontsize=10];")
        sb.appendLine()

        val nodeSet = nodeIds.toSet()

        // Nodes with properties as labels
        for (id in nodeIds) {
            val node = storage.getNode(id) ?: continue
            val label = buildString {
                append(id)
                if (node.labels.isNotEmpty()) append("\\n[${node.labels.joinToString(",")}]")
                for ((k, v) in node.properties.entries.take(5)) {
                    append("\\n$k=$v")
                }
            }
            sb.appendLine("  \"$id\" [label=\"$label\"];")
        }

        sb.appendLine()

        // Edges
        for (sourceId in nodeIds) {
            val edges = if (edgeType != null) {
                storage.getOutboundEdges(sourceId, edgeType)
            } else {
                storage.getAllOutboundEdges(sourceId)
            }
            for (edge in edges) {
                if (edge.targetId !in nodeSet) continue
                val label = buildString {
                    append(edge.type)
                    val weight = edge.properties["weight"]
                    if (weight != null) append(" ($weight)")
                }
                sb.appendLine("  \"${edge.sourceId}\" -> \"${edge.targetId}\" [label=\"$label\"];")
            }
        }

        sb.appendLine("}")
        return sb.toString()
    }

    /**
     * Exports a subgraph to GraphML format.
     *
     * GraphML is an XML-based format supported by:
     * - Gephi (network analysis)
     * - yEd (graph visualization)
     * - Cytoscape (biological networks)
     * - NetworkX (Python graph library)
     *
     * @param storage The graph storage engine.
     * @param nodeIds The nodes to include.
     * @param edgeType The relationship type to export (null = all types).
     * @return A string in GraphML XML format.
     */
    fun toGraphML(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeType: String? = null
    ): String {
        val sb = StringBuilder()
        val nodeSet = nodeIds.toSet()

        sb.appendLine("""<?xml version="1.0" encoding="UTF-8"?>""")
        sb.appendLine("""<graphml xmlns="http://graphml.graphstudio.org/xmlns">""")

        // Declare attribute keys
        sb.appendLine("""  <key id="label" for="node" attr.name="label" attr.type="string"/>""")
        sb.appendLine("""  <key id="labels" for="node" attr.name="labels" attr.type="string"/>""")
        sb.appendLine("""  <key id="type" for="edge" attr.name="type" attr.type="string"/>""")
        sb.appendLine("""  <key id="weight" for="edge" attr.name="weight" attr.type="double"/>""")

        // Collect unique property keys from nodes
        val propKeys = mutableSetOf<String>()
        val nodes = nodeIds.mapNotNull { id -> storage.getNode(id)?.let { id to it } }
        for ((_, node) in nodes) propKeys.addAll(node.properties.keys)
        for (key in propKeys) {
            sb.appendLine("""  <key id="np_$key" for="node" attr.name="$key" attr.type="string"/>""")
        }

        sb.appendLine("""  <graph id="G" edgedefault="directed">""")

        // Nodes
        for ((id, node) in nodes) {
            sb.appendLine("""    <node id="$id">""")
            sb.appendLine("""      <data key="label">${escapeXml(id)}</data>""")
            if (node.labels.isNotEmpty()) {
                sb.appendLine("""      <data key="labels">${escapeXml(node.labels.joinToString(","))}</data>""")
            }
            for ((k, v) in node.properties) {
                sb.appendLine("""      <data key="np_$k">${escapeXml(v)}</data>""")
            }
            sb.appendLine("""    </node>""")
        }

        // Edges
        var edgeCounter = 0
        for (sourceId in nodeIds) {
            val edges = if (edgeType != null) {
                storage.getOutboundEdges(sourceId, edgeType)
            } else {
                storage.getAllOutboundEdges(sourceId)
            }
            for (edge in edges) {
                if (edge.targetId !in nodeSet) continue
                sb.appendLine("""    <edge id="e${edgeCounter++}" source="${edge.sourceId}" target="${edge.targetId}">""")
                sb.appendLine("""      <data key="type">${escapeXml(edge.type)}</data>""")
                val weight = edge.properties["weight"]
                if (weight != null) {
                    sb.appendLine("""      <data key="weight">$weight</data>""")
                }
                sb.appendLine("""    </edge>""")
            }
        }

        sb.appendLine("""  </graph>""")
        sb.appendLine("""</graphml>""")
        return sb.toString()
    }

    /**
     * Exports a subgraph to valid Mermaid markdown syntax.
     *
     * @param storage The graph storage engine.
     * @param nodeIds Nodes to include in the diagram.
     * @param edgeType Optional edge type filter (null = all edge types).
     * @param direction Diagram direction: "TD" (Top-Down), "LR" (Left-Right), "BT", "RL".
     * @param groupBySubgraphs Whether to group nodes into subgraphs by their layer/label.
     * @param maxLabelLength Maximum character length for node labels before truncation.
     * @return Valid Mermaid markdown code block string.
     */
    fun toMermaid(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeType: String? = null,
        direction: String = "TD",
        groupBySubgraphs: Boolean = true,
        maxLabelLength: Int = 40
    ): String {
        if (nodeIds.isEmpty()) {
            return "graph $direction\n  %% empty"
        }

        val sb = StringBuilder()
        sb.appendLine("graph $direction")
        val nodeSet = nodeIds.toSet()

        fun sanitizeId(id: String): String = id.replace(Regex("[^a-zA-Z0-9_]"), "_")
        fun escapeLabel(text: String): String {
            val truncated = if (text.length > maxLabelLength) text.substring(0, maxLabelLength) + "..." else text
            return truncated
                .replace("\"", "#quot;")
                .replace("<", "#lt;")
                .replace(">", "#gt;")
                .replace("\r", " ")
                .replace("\n", " ")
                .replace("\t", " ")
        }

        val nodes = nodeIds.map { id -> id to storage.getNode(id) }

        if (groupBySubgraphs) {
            val groups = nodes.groupBy { (id, node) ->
                when {
                    node?.labels?.any { it == "L3_Persona" || it.startsWith("L3_Persona") } == true || id.startsWith("l3:persona") ->
                        "L3_Persona" to "L3: Persona"
                    node?.labels?.any { it == "L3_Invariant" || it.startsWith("L3_Invariant") } == true || id.startsWith("l3:inv") ->
                        "L3_Invariant" to "L3: Invariants"
                    node?.labels?.any { it.startsWith("L3") } == true || id.startsWith("l3:") ->
                        "L3_Persona_Invariants" to "L3: Persona & Invariants"
                    node?.labels?.any { it.startsWith("L2") } == true || id.startsWith("l2:") ->
                        "L2_Scenario" to "L2: Scenario Clusters"
                    node?.labels?.any { it.startsWith("L1") } == true || id.startsWith("l1:") ->
                        "L1_Fact" to "L1: Atomic Facts"
                    node?.labels?.any { it.startsWith("L0") } == true || id.startsWith("l0:") ->
                        "L0_Evidence" to "L0: Raw Evidence"
                    node != null && node.labels.isNotEmpty() ->
                        sanitizeId(node.labels.first()) to node.labels.first()
                    else -> "General" to "Nodes"
                }
            }

            for ((groupKey, groupNodes) in groups) {
                val (groupId, groupTitle) = groupKey
                sb.appendLine("  subgraph $groupId[\"$groupTitle\"]")
                for ((id, node) in groupNodes) {
                    val rawText = node?.properties?.get("text")
                        ?: node?.properties?.get("title")
                        ?: node?.properties?.get("statement")
                        ?: node?.properties?.get("name")
                        ?: id
                    val safeId = sanitizeId(id)
                    val label = escapeLabel(rawText)
                    sb.appendLine("    $safeId[\"$label\"]")
                }
                sb.appendLine("  end")
            }
        } else {
            for ((id, node) in nodes) {
                val rawText = node?.properties?.get("text")
                    ?: node?.properties?.get("title")
                    ?: node?.properties?.get("statement")
                    ?: node?.properties?.get("name")
                    ?: id
                val safeId = sanitizeId(id)
                val label = escapeLabel(rawText)
                sb.appendLine("  $safeId[\"$label\"]")
            }
        }

        // Edges
        for (sourceId in nodeIds) {
            val edges = if (edgeType != null) {
                storage.getOutboundEdges(sourceId, edgeType)
            } else {
                storage.getAllOutboundEdges(sourceId)
            }
            for (edge in edges) {
                if (edge.targetId !in nodeSet) continue
                val safeSource = sanitizeId(edge.sourceId)
                val safeTarget = sanitizeId(edge.targetId)
                val weight = edge.properties["weight"]
                val edgeLabel = if (weight != null) "${edge.type} ($weight)" else edge.type
                val cleanEdgeLabel = edgeLabel.replace("\"", "")
                sb.appendLine("  $safeSource -->|$cleanEdgeLabel| $safeTarget")
            }
        }

        return sb.toString()
    }

    private fun escapeXml(s: String): String = s
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
}
