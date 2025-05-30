/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import java.sql.Connection

class Transaction(private var connection: Connection) {

	fun begin(transactionLevel: Int = Connection.TRANSACTION_READ_COMMITTED) {
		connection.autoCommit = false
		connection.transactionIsolation = transactionLevel
	}

	fun commit() {
		connection.commit()
	}

	fun rollback() {
		connection.rollback()
	}
}

suspend fun <T> transaction(conn: Connection, block: suspend () -> T): T {
	val transaction = Transaction(conn)
	try {
		transaction.begin()
		val res = block()
		transaction.commit()
		return res
	} catch (e: Exception) {
		transaction.rollback()
		throw e
	}
}