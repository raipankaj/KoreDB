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

package com.pankaj.koredb.log

/**
 * Log levels for KoreLogger.
 */
enum class KoreLogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR,
    NONE
}

/**
 * Pluggable logger interface for KoreDB.
 */
interface KoreLogger {
    fun debug(message: String)
    fun info(message: String)
    fun warn(message: String, throwable: Throwable? = null)
    fun error(message: String, throwable: Throwable? = null)

    companion object {
        @Volatile
        var minLevel: KoreLogLevel = KoreLogLevel.INFO

        @Volatile
        var factory: (String) -> KoreLogger = { tag -> DefaultKoreLogger(tag) }

        fun getLogger(tag: String): KoreLogger = factory(tag)
    }
}

/**
 * Default console logger implementation.
 */
class DefaultKoreLogger(private val tag: String) : KoreLogger {
    override fun debug(message: String) {
        if (KoreLogger.minLevel <= KoreLogLevel.DEBUG) {
            println("[DEBUG] [$tag] $message")
        }
    }

    override fun info(message: String) {
        if (KoreLogger.minLevel <= KoreLogLevel.INFO) {
            println("[INFO] [$tag] $message")
        }
    }

    override fun warn(message: String, throwable: Throwable?) {
        if (KoreLogger.minLevel <= KoreLogLevel.WARN) {
            if (throwable != null) {
                println("[WARN] [$tag] $message - ${throwable.message}")
            } else {
                println("[WARN] [$tag] $message")
            }
        }
    }

    override fun error(message: String, throwable: Throwable?) {
        if (KoreLogger.minLevel <= KoreLogLevel.ERROR) {
            if (throwable != null) {
                println("[ERROR] [$tag] $message - ${throwable.message}")
                throwable.printStackTrace()
            } else {
                println("[ERROR] [$tag] $message")
            }
        }
    }
}

/**
 * Android Logcat logger implementation with safe JVM fallback.
 */
class AndroidLogcatLogger(private val tag: String) : KoreLogger {
    override fun debug(message: String) {
        if (KoreLogger.minLevel <= KoreLogLevel.DEBUG) {
            try {
                android.util.Log.d(tag, message)
            } catch (_: Throwable) {
                println("[DEBUG] [$tag] $message")
            }
        }
    }

    override fun info(message: String) {
        if (KoreLogger.minLevel <= KoreLogLevel.INFO) {
            try {
                android.util.Log.i(tag, message)
            } catch (_: Throwable) {
                println("[INFO] [$tag] $message")
            }
        }
    }

    override fun warn(message: String, throwable: Throwable?) {
        if (KoreLogger.minLevel <= KoreLogLevel.WARN) {
            try {
                if (throwable != null) {
                    android.util.Log.w(tag, message, throwable)
                } else {
                    android.util.Log.w(tag, message)
                }
            } catch (_: Throwable) {
                if (throwable != null) {
                    println("[WARN] [$tag] $message - ${throwable.message}")
                } else {
                    println("[WARN] [$tag] $message")
                }
            }
        }
    }

    override fun error(message: String, throwable: Throwable?) {
        if (KoreLogger.minLevel <= KoreLogLevel.ERROR) {
            try {
                if (throwable != null) {
                    android.util.Log.e(tag, message, throwable)
                } else {
                    android.util.Log.e(tag, message)
                }
            } catch (_: Throwable) {
                if (throwable != null) {
                    println("[ERROR] [$tag] $message - ${throwable.message}")
                    throwable.printStackTrace()
                } else {
                    println("[ERROR] [$tag] $message")
                }
            }
        }
    }
}
