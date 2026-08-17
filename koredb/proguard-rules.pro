# Suppress warnings for classes that are handled by R8/Desugaring but not present in Android SDK
-dontwarn java.lang.invoke.StringConcatFactory

# Preserve the public API of KoreDB
-keep public class com.pankaj.koredb.db.KoreDatabase {
    public <methods>;
    public <fields>;
}

-keep public class com.pankaj.koredb.db.KoreAndroid {
    public <methods>;
    public <fields>;
}

-keep public class com.pankaj.koredb.core.KoreCollection {
    public <methods>;
    public <fields>;
}

-keep public class com.pankaj.koredb.core.KoreVectorCollection {
    public <methods>;
    public <fields>;
}

-keep public class com.pankaj.koredb.core.VectorCollectionConfig {
    public <methods>;
    public <fields>;
}

-keep public class com.pankaj.koredb.core.VectorCollectionConfig$Builder {
    public <methods>;
    public <fields>;
}

-keep public class com.pankaj.koredb.hnsw.VectorFilterBuilder {
    public <methods>;
    public <fields>;
}

# Keep the top-level DSL functions and predicates in the .hnsw package
-keep public class com.pankaj.koredb.hnsw.VectorFilterKt {
    public static <methods>;
}

-keep public class com.pankaj.koredb.hnsw.FilterPredicate { *; }
-keep public class com.pankaj.koredb.hnsw.*Predicate { *; }

-keep public enum com.pankaj.koredb.hnsw.DistanceMetric {
    *;
}

-keep public class com.pankaj.koredb.core.KoreSerializer {
    public <methods>;
    public <fields>;
}

# Preserve the Graph API if it's public
-keep public class com.pankaj.koredb.graph.** {
    public <methods>;
    public <fields>;
}

# Preserve the Bridge API
-keep public class com.pankaj.koredb.bridge.** {
    public <methods>;
    public <fields>;
}

# Preserve the Stream API
-keep public class com.pankaj.koredb.stream.** {
    public <methods>;
    public <fields>;
}

# Preserve the Key-Value Cache API
-keep public class com.pankaj.koredb.kv.** {
    public <methods>;
    public <fields>;
}

# Preserve the Crypto API
-keep public class com.pankaj.koredb.crypto.** {
    public <methods>;
    public <fields>;
}

# Preserve the Compression API
-keep public class com.pankaj.koredb.compression.** {
    public <methods>;
    public <fields>;
}

# Preserve the FTS API
-keep public class com.pankaj.koredb.fts.** {
    public <methods>;
    public <fields>;
}

# Preserve the Exporter API
-keep public class com.pankaj.koredb.exporter.** {
    public <methods>;
    public <fields>;
}

# Keep serialization-related metadata if you use kotlinx.serialization
-keepattributes *Annotation*, InnerClasses, EnclosingMethod, Signature, Exceptions
-keepclassmembers class * {
    @kotlinx.serialization.Serializable *;
}

# Optional: Preserve line numbers for stack traces but obfuscate source file names
-keepattributes SourceFile,LineNumberTable
-renamesourcefileattribute SourceFile

