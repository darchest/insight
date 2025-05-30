/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class ComparisonOperation<javaType: Any, sqlT: SqlType>(val left: SqlValue<*, *>, val operator: Operator, val right: SqlValue<*, *>, javaClass: Class<javaType>, sqlType: sqlT): SqlValue<javaType, sqlT>(javaClass, sqlType) {
	enum class Operator {
		EQ,
		NEQ,
		LT,
		LE,
		GT,
		GE
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		left.writeSql(builder, vendor, params)
		builder.append(" ")
		builder.append(operatorSql(operator))
		builder.append(" ")
		right.writeSql(builder, vendor, params)
	}

	fun operatorSql(op: Operator): String {
		return when (op) {
			Operator.EQ -> "="
			Operator.NEQ -> "<>"
			Operator.LT -> "<"
			Operator.LE -> "<="
			Operator.GT -> ">"
			Operator.GE -> ">="
		}
	}

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		left.innerColumns(array)
		right.innerColumns(array)
	}
}