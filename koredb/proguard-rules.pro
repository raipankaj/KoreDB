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

-keep public class com.pankaj.koredb.core.KoreSerializer {
    public <methods>;
    public <fields>;
}

# Preserve the Graph API if it's public
-keep public class com.pankaj.koredb.graph.** {
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

