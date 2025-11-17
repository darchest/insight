/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.ddl

import mu.KotlinLogging
import org.darchest.insight.*

class CreateTable(val table: Table): SqlPrintable {

	private val logger = KotlinLogging.logger {}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("CREATE TABLE IF NOT EXISTS ")
		table.writeSql(builder, vendor, params)
		builder.append("\n(\n\t")
		val cols = table.columns()
		val iter = cols.iterator()
		if (!iter.hasNext())
			throw RuntimeException("No columns in table ${table.sqlName}")
		var col = iter.next()
		writeColumn(col, builder, vendor, params)
		while(iter.hasNext()) {
			col = iter.next()
			builder.append(",\n\t")
			writeColumn(col, builder, vendor, params)
		}
		val consts = table.constraints().iterator()
		while (consts.hasNext()) {
			val c = consts.next()
			builder.append(",\n\t")
			c.writeSql(builder, vendor, params)
		}
		builder.append("\n);")
	}

	private suspend fun writeColumn(col: TableColumn<*, *>, builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append(col.name, ' ', col.getSqlStringType())
		col.length?.apply { builder.append("(", this, ")") }
		builder.append(" NOT NULL")
		val def = col.default()
		if (def != null) {
			builder.append(" DEFAULT ")
			builder.append(SqlTypeConvertersRegistry.javaToSql(def.javaClass, def.sqlClass, def.getValue()))
		}
	}

	suspend fun execute(): Int {
		val vendor = ConnectionManager.getVendor(table.connectionName)
		val sqlTimeFrom = System.currentTimeMillis()
		val (sql, params) = getSql(vendor)
		logger.debug { "Sql created in ${System.currentTimeMillis() - sqlTimeFrom } mills:\n${sql}" }
		val connection = ConnectionManager.getConnection()
		connection.createStatement().use { statement ->
			return statement.executeUpdate(sql)
		}
	}
}
