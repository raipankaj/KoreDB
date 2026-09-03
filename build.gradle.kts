// Top-level build file where you can add configuration options common to all sub-projects/modules.
plugins {
    alias(libs.plugins.android.application) apply false
    alias(libs.plugins.kotlin.compose) apply false
    alias(libs.plugins.android.library) apply false
    alias(libs.plugins.ksp) apply false
    alias(libs.plugins.dokka) apply false
}

tasks.register("installAgentSkills") {
    group = "help"
    description = "Installs KoreDB AI Agent skills to local user configuration directories (~/.gemini/antigravity/skills and ~/.claude/skills)."
    doLast {
        val userHome = System.getProperty("user.home")
        val targetDirs = listOf(
            File(userHome, ".gemini/antigravity/skills"),
            File(userHome, ".claude/skills"),
            File(userHome, ".agents/skills")
        )
        val sourceDir = file("skills")
        if (sourceDir.exists()) {
            for (targetDir in targetDirs) {
                try {
                    targetDir.mkdirs()
                    sourceDir.listFiles()?.filter { it.isDirectory }?.forEach { skillFolder ->
                        val dest = File(targetDir, skillFolder.name)
                        skillFolder.copyRecursively(dest, overwrite = true)
                    }
                    println("Successfully installed KoreDB skills to: ${targetDir.absolutePath}")
                } catch (e: Exception) {
                    // Ignore locations that cannot be accessed
                }
            }
        }
    }
}