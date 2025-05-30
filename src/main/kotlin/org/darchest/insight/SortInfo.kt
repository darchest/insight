/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

class SortInfo(val expr: SqlValue<*, *>, val direction: Direction): SqlPrintable {
	enum class Direction { ASC, DESC }

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		expr.writeSql(builder, vendor, params)
		builder.append(" ")
		builder.append(if (direction == Direction.ASC) "ASC" else "DESC")
	}
}