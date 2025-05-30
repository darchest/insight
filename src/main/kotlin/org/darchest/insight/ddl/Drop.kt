/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.ddl

import org.darchest.insight.*

class DropTable(val table: Table): SqlPrintable {

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("DELETE TABLE ")
		table.writeSql(builder, vendor, params)
	}

	suspend fun execute(): Int {
		val vendor = ConnectionManager.getVendor()
		val (sql, params) = getSql(vendor)
		val connection = ConnectionManager.getConnection()
		val statement = connection.createStatement()
		val res = statement.executeUpdate(sql)
		connection.close()
		return res
	}
}

fun dropTable(table: Table): DropTable {
	return DropTable(table)
}