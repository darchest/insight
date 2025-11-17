/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.dml

import mu.KotlinLogging
import org.darchest.insight.*
import java.util.*
import java.util.stream.Collectors

class Insert: SqlPrintable {
	private val logger = KotlinLogging.logger {}

	private val data = mutableListOf<Table>()

	fun addRows(vararg data: Table): Insert {
		this.data.addAll(data)
		return this
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("INSERT INTO ")
		data[0].writeSql(builder, vendor, params)
		builder.append(" (")
		val cols = settedColumns()
		builder.append(Arrays.stream(cols).collect(Collectors.joining(", ")))
		builder.append(") VALUES (")
		val iter = cols.iterator()
		var col = iter.next()
		builder.appendSqlValueParam(data[0].column(col), params)
		while (iter.hasNext()) {
			iter.next()
			builder.append(",")
			builder.appendSqlValueParam(data[0].column(col), params)
		}
		builder.append(")")
	}

	private fun settedColumns(): Array<String> {
		val columns = mutableSetOf<String>()
		for (t in data) {
			for (f in t.columns()) {
				if (f.state == SqlValue.State.VALUE_SET)
					columns.add(f.name)
			}
		}
		return columns.toTypedArray()
	}

	suspend fun execute(): Int {
		val vendor = ConnectionManager.getVendor(data[0].connectionName)
		var cnt = 0
		val cols = settedColumns()
		val sqlTimeFrom = System.currentTimeMillis()
		val (sql, params) = getSql(vendor)
		logger.debug { "Sql created in ${System.currentTimeMillis() - sqlTimeFrom } mills:\n${sql}" }
		val connection = ConnectionManager.getConnection()
		val statement = connection.prepareStatement(sql)
		for (d in data.indices) {
			cols.forEachIndexed { ind, colName ->
				val col = data[d].column(colName)
				params[ind] = col
			}

			var ind = 0
			params.forEach {
				SqlTypeConvertersRegistry.javaToPrepSql(it.javaClass, it.sqlClass, statement, ++ind, it.getValue())
			}
			logger.trace { "Prepared statement Insert:\n${statement}" }
			cnt += statement.executeUpdate()
		}
		return cnt
	}
}

fun insert(vararg data: Table): Insert {
	val ins = Insert()
	ins.addRows(*data)
	return ins
}