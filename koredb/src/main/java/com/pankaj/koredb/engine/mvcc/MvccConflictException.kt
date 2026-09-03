package com.pankaj.koredb.engine.mvcc

/**
 * Exception thrown when a write-write transaction collision occurs under Snapshot Isolation.
 */
class MvccConflictException(message: String) : RuntimeException(message)


