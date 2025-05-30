/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.dml

import mu.KotlinLogging
import org.darchest.insight.*

class Delete(val table: Table): SqlPrintable {
	private val logger = KotlinLogging.logger {}

	private var whereExpr: SqlValue<*, *>? = null

	suspend fun where(expr: suspend () -> SqlValue<*, *>?): Delete {
		whereExpr = expr.invoke()
		return this
	}

	fun where(expr: SqlValue<*, *>?): Delete {
		whereExpr = expr
		return this
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("DELETE FROM ")
		table.writeSql(builder, vendor, params)
		builder.append("\r\n\tWHERE ")
		whereExpr!!.writeSql(builder, vendor, params)
	}

	suspend fun execute(): Int {
		val vendor = ConnectionManager.getVendor(table.connectionName)
		val sqlTimeFrom = System.currentTimeMillis()
		val (sql, params) = getSql(vendor)
		logger.debug { "Sql created in ${System.currentTimeMillis() - sqlTimeFrom } mills:\n${sql}" }
		val queryTimeFrom = System.currentTimeMillis()
		val connection = ConnectionManager.getConnection()
		val statement = connection.prepareStatement(sql)
		var ind = 0
		params.forEach {
			SqlTypeConverter.javaToPrepSql(it.javaClass, it.sqlClass, statement, ++ind, it.getValue())
		}
		logger.trace { "Prepared statement DELETE:\n${statement}" }
		val res = statement.executeUpdate()
		logger.debug { "DELETE query time: ${System.currentTimeMillis() - queryTimeFrom} mills" }
		statement.close()
		return res
	}
}

suspend fun delete(table: Table, expr: suspend () -> SqlValue<*, *>): Delete {
	val del = Delete(table)
	del.where(expr)
	return del
}