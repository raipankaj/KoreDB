# KoreDB Agent Skills 🧠🤖

Welcome to the **KoreDB Agent Skills** package. This directory equips modern AI coding assistants (such as Google Antigravity, Claude Code, Cursor, GitHub Copilot, Gemini CLI, and custom LLM agents) with deep, comprehensive, and non-hallucinatory understanding of **KoreDB**.

With these skills, an agent can autonomously write production-grade Kotlin code, implement sub-millisecond vector similarity search, traverse property graphs, build hybrid RAG pipelines, manage ACID MVCC transactions, and migrate legacy Room/SQLite codebases without human trial-and-error.

---

## 📚 Available Skills

| Skill Directory | Skill Name | Primary Purpose & Capabilities |
| :--- | :--- | :--- |
| **[`koredb-setup/`](koredb-setup/SKILL.md)** | `koredb-setup` | Dependencies, DI (Hilt, Koin), `KoreAndroid.builder`, encryption (AES-GCM-256), LZ4 compression, memory trimming, and storage directories. |
| **[`koredb-document/`](koredb-document/SKILL.md)** | `koredb-document` | `@Serializable` entities, CBOR binary collections, secondary indexes, numeric range queries, BM25 text search, TTL, and Flow observation. |
| **[`koredb-vector/`](koredb-vector/SKILL.md)** | `koredb-vector` | High-dimensional HNSW vector search, 16-lane SIMD distance metrics, SQ8 / Product Quantization, off-heap mmap, and metadata filtering. |
| **[`koredb-graph/`](koredb-graph/SKILL.md)** | `koredb-graph` | Property nodes, directed weighted edges, dual bidirectional indexing, key-only traversal (`toIdList`), cascading deletes, Dijkstra, and PageRank. |
| **[`koredb-hybrid-rag/`](koredb-hybrid-rag/SKILL.md)** | `koredb-hybrid-rag` | `GraphVectorBridge`, combining dense vector embeddings with graph relationships, Reciprocal Rank Fusion (RRF), and on-device LLM context building. |
| **[`koredb-transactions/`](koredb-transactions/SKILL.md)** | `koredb-transactions` | ACID MVCC Snapshot Isolation, First-Committer-Wins conflict resolution, direct commit pipeline, Change Data Capture (CDC), and backup snapshots. |
| **[`koredb-migration-room/`](koredb-migration-room/SKILL.md)** | `koredb-migration-room` | Step-by-step Room `@Entity`, `@Dao`, and SQL query conversion to KoreDB collections, plus zero-downtime background migration workers. |

---

## 🚀 How to Install & Use These Skills

You can install KoreDB skills using your favorite CLI, package manager, or Gradle:

### Option 1: Antigravity CLI (`agy`)
Install skills directly from the remote repository into your local or workspace agent:

```bash
# Install all KoreDB skills
agy skill install https://github.com/raipankaj/KoreDB --path skills

# Or install a specific skill (e.g. vector search or hybrid rag)
agy skill install https://github.com/raipankaj/KoreDB --path skills/koredb-vector
agy skill install https://github.com/raipankaj/KoreDB --path skills/koredb-hybrid-rag
```

### Option 2: Universal Skills Package Manager (`npx skills-cli`)
For teams using cross-agent tooling (Cursor, Claude Code, Gemini):

```bash
# Install from GitHub
npx skills-cli add https://github.com/raipankaj/KoreDB --path skills

# Or install to global configuration
npx skills-cli add https://github.com/raipankaj/KoreDB --path skills --global
```

### Option 3: Android Gradle Task (Built-in)
If you already have KoreDB in your project or cloned locally:

```bash
./gradlew installAgentSkills
```
*Copies all skills directly into `~/.gemini/antigravity/skills/`, `~/.claude/skills/`, and `~/.agents/skills/`.*

### Option 4: Git Submodule (Project Level)
To version-control the skills directly within your own Android application team repo:

```bash
git submodule add https://github.com/raipankaj/KoreDB.git .koredb
mkdir -p .agents
ln -s ../.koredb/skills .agents/skills
```

---

## 💡 Example Agent Prompts Enabled by These Skills

Once installed, you can prompt your AI assistant with high-level requests, and it will generate exact, idiomatic KoreDB code:

* *"Add an on-device vector collection for user notes using 384-dimensional embeddings and Cosine similarity, with SQ8 quantization enabled."*
* *"Create a social graph of users and follows relationships, and write a function to find friends-of-friends using the key-only traversal fast-path."*
* *"Migrate our existing Room ProductDao and ProductEntity to KoreDB, including numeric range indexing on price and a background migration worker."*
* *"Build a Hybrid Graph RAG pipeline using GraphVectorBridge that retrieves articles similar to the user's query and reranks them with Reciprocal Rank Fusion."*
