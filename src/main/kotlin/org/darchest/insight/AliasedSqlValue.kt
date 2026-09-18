/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

/**
 * Wraps a [SqlValue] for use in a SELECT list with an SQL alias (`expr AS "name"`).
 */
class AliasedSqlValue<javaType : Any, sqlT : SqlType>(
	val value: SqlValue<javaType, sqlT>,
	val alias: String
) : SqlValue<javaType, sqlT>(value.javaClass, value.sqlType) {

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		value.writeSql(builder, vendor, params)
		builder.append(" AS ")
		vendor.writeSqlAlias(alias, builder)
	}

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		value.innerColumns(array)
	}

	override fun collectReferencedSources(out: MutableSet<SqlDataSource>) {
		value.collectReferencedSources(out)
	}
}

/** Select-list alias: `expr AS "name"`. */
fun <J : Any, S : SqlType> SqlValue<J, S>.alias(name: String): AliasedSqlValue<J, S> =
	AliasedSqlValue(this, name)
