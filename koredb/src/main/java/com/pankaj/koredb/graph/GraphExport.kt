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

    private fun escapeXml(s: String): String = s
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
}
