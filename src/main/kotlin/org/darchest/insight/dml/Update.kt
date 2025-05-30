/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.dml

import mu.KotlinLogging
import org.darchest.insight.*

class Update<T: Table>(val table: T): SqlPrintable {
	private val logger = KotlinLogging.logger {}

	private var whereExpr: SqlValue<*, *>? = null

	suspend fun where(expr: suspend () -> SqlValue<*, *>?): Update<T> {
		whereExpr = expr.invoke()
		return this
	}

	fun where(expr: SqlValue<*, *>?): Update<T> {
		whereExpr = expr
		return this
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("UPDATE ")
		table.writeSql(builder, vendor, params)
		builder.append(" SET\r\n\t")
		val cols = settedColumns()
		val iter = cols.iterator()
		var c = iter.next()
		writeSqlColumn(c, builder, vendor, params)
		while (iter.hasNext()) {
			builder.append(",\r\n\t")
			c = iter.next()
			writeSqlColumn(c, builder, vendor, params)
		}
		builder.append("\r\nWHERE ")
		whereExpr!!.writeSql(builder, vendor, params)
	}

	private suspend fun writeSqlColumn(c: String, builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		val col = table.column(c)
		builder.append("\"$c\"=")
		if (col.state == SqlValue.State.EXPR_SET)
			builder.append(col.settedExpr!!.getSql(vendor))
		else {
			builder.appendSqlValueParam(col, params)
		}
	}

	private fun settedColumns(): Array<String> {
		val columns = mutableSetOf<String>()
		for (f in table.columns()) {
			if (f.state == SqlValue.State.VALUE_SET || f.state == SqlValue.State.EXPR_SET)
				columns.add(f.name)
		}
		return columns.toTypedArray()
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
		logger.trace { "Prepared statement UPDATE:\n${statement}" }
		val res = statement.executeUpdate()
		logger.debug { "UPDATE query time: ${System.currentTimeMillis() - queryTimeFrom} mills" }
		return res
	}
}


suspend fun <T: Table> update(data: T, expr: suspend () -> SqlValue<*, *>): Update<T> {
	val upd = Update(data)
	upd.where(expr)
	return upd
}
