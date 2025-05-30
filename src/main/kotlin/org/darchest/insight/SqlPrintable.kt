/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

interface SqlPrintable {

	suspend fun getSql(vendor: Vendor): Pair<String, MutableList<SqlValue<*, *>>> {
		val builder = StringBuilder(1024)
		val params = mutableListOf<SqlValue<*, *>>()
		writeSql(builder, vendor, params)
		return Pair(builder.toString(), params)
	}

	suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>)
}

fun StringBuilder.appendSqlValueParam(value: SqlValue<*, *>, params: MutableList<SqlValue<*, *>>) {
	append("?")
	params.add(value)
}