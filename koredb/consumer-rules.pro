# KoreDB ProGuard Rules
# These rules are automatically applied to apps that consume KoreDB.

# Keep the public API for the database and collections
-keep class com.pankaj.koredb.db.KoreDatabase { *; }
-keep class com.pankaj.koredb.db.KoreAndroid { *; }
-keep class com.pankaj.koredb.core.KoreCollection { *; }
-keep class com.pankaj.koredb.core.KoreVectorCollection { *; }
-keep class com.pankaj.koredb.core.VectorCollectionConfig { *; }
-keep class com.pankaj.koredb.core.VectorCollectionConfig$Builder { *; }
-keep class com.pankaj.koredb.hnsw.VectorFilterBuilder { *; }
-keep class com.pankaj.koredb.hnsw.VectorFilterKt { *; }
-keep class com.pankaj.koredb.hnsw.FilterPredicate { *; }
-keep class com.pankaj.koredb.hnsw.*Predicate { *; }
-keep enum com.pankaj.koredb.hnsw.DistanceMetric { *; }
-keep class com.pankaj.koredb.core.KoreSerializer { *; }
-keep class com.pankaj.koredb.graph.** { *; }
-keep class com.pankaj.koredb.bridge.** { *; }
-keep class com.pankaj.koredb.engine.mvcc.** { *; }
-keep class com.pankaj.koredb.compression.** { *; }
-keep class com.pankaj.koredb.foundation.** { *; }
-keep class com.pankaj.koredb.core.SimdVectorMath { *; }
-keep class com.pankaj.koredb.hnsw.ProductQuantizer { *; }
-keep class com.pankaj.koredb.cdc.** { *; }
-keep class com.pankaj.koredb.engine.IntegrityVerifier { *; }
-keep class com.pankaj.koredb.engine.IntegrityReport { *; }
-keep class com.pankaj.koredb.engine.KoreDBException { *; }
-keep class com.pankaj.koredb.engine.DatabaseLockedException { *; }
-keep class com.pankaj.koredb.engine.DiskSpaceExhaustedException { *; }

# Prevent R8 from removing the companion objects which contain important constants/helpers
-keepclassmembers class com.pankaj.koredb.engine.KoreDB$Companion { *; }
-keepclassmembers class com.pankaj.koredb.foundation.SSTable$Companion { *; }

