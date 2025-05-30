/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.part

import org.darchest.insight.SqlPrintable
import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

class SqlPartsArray<T: SqlPrintable>(): ArrayList<T>(), SqlPrintable {

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		if (isEmpty())
			return
		this[0].writeSql(builder, vendor, params)
		for (i in 1 until size) {
			builder.append(", ")
			this[i].writeSql(builder, vendor, params)
		}
	}
}