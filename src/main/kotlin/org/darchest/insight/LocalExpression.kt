/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class LocalExpression<javaType: Any>(javaClass: Class<javaType>, private val innerColumns: List<TableColumn<*, *>>,  private val fn: suspend () -> javaType?): SqlValue<javaType, SqlTypeNone>(javaClass, SqlTypeNone()), SqlValueNotNullGetter<javaType> {

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		array.addAll(innerColumns)
	}

	override suspend fun getValue(): javaType? = fn()

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("0")
	}
}