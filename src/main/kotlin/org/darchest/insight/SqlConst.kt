/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

class SqlConst<javaType: Any, sqlT: SqlType>(val valuee: javaType?, javaClass: Class<javaType>, sqlType: sqlT): SqlValue<javaType, sqlT>(javaClass, sqlType) {
	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.appendSqlValueParam(this, params)
	}

	override var state = State.VALUE_SET

	override suspend fun getValue(): javaType? {
		return valuee
	}
}