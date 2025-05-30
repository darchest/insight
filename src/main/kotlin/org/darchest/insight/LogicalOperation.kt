/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class LogicalOperation<javaType: Any, sqlT: SqlType>(val operator: Operator, val values: Collection<SqlValue<*, *>>, javaClass: Class<javaType>, sqlType: sqlT): SqlValue<javaType, sqlT>(javaClass, sqlType) {
	enum class Operator {
		AND,
		OR
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		val iterator = values.iterator()

		writeValue(builder, vendor, iterator.next(), params)

		while (iterator.hasNext()) {
			builder.append(" ")
			builder.append(operatorSql(operator))
			builder.append(" ")

			writeValue(builder, vendor, iterator.next(), params)
		}
	}

	private suspend fun writeValue(builder: StringBuilder, vendor: Vendor, value: SqlValue<*, *>, params: MutableList<SqlValue<*, *>>) {
		val brace = value is LogicalOperation
		if (brace) builder.append('(')
		value.writeSql(builder, vendor, params)
		if (brace) builder.append(')')
	}

	private fun operatorSql(op: Operator): String {
		return when (op) {
			Operator.AND -> "AND"
			Operator.OR -> "OR"
		}
	}

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		values.forEach { it.innerColumns(array) }
	}
}