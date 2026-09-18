/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.expression

import org.darchest.insight.Expression
import org.darchest.insight.OneColumnSelect
import org.darchest.insight.SqlDataSource
import org.darchest.insight.SqlType
import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

/**
 * Scalar subquery expression built from a [OneColumnSelect].
 * Emits `(SELECT …)` for use in SELECT list / WHERE / HAVING.
 */
class SelectExpression<javaType : Any, sqlType : SqlType>(
	val select: OneColumnSelect<*>,
	javaClass: Class<javaType>,
	sqlType: sqlType
) : Expression<javaType, sqlType>(javaClass, sqlType) {

	override suspend fun writeSql(
		builder: StringBuilder,
		vendor: Vendor,
		params: MutableList<SqlValue<*, *>>
	) {
		builder.append("(")
		select.writeSql(builder, vendor, params)
		builder.append(")")
	}

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		// Do not register the subquery as a joinable column for the outer query.
		// Inner table columns must not pull JOINs into the outer FROM.
	}

	override fun collectReferencedSources(out: MutableSet<SqlDataSource>) {
		// Uncorrelated: outer join discovery ignores the subquery's own sources.
	}
}

/**
 * Build a typed scalar subquery expression.
 * Pass [of] (typically the outer column compared to the subquery) to fix expression types.
 */
fun <J : Any, S : SqlType> OneColumnSelect<*>.asExpression(of: SqlValue<J, S>): SelectExpression<J, S> =
	SelectExpression(this, of.javaClass, of.sqlType)

/** Scalar subquery factory with types taken from [of]. */
fun <J : Any, S : SqlType> subSelect(select: OneColumnSelect<*>, of: SqlValue<J, S>): SelectExpression<J, S> =
	select.asExpression(of)

/**
 * Untyped scalar subquery (types taken from the single select-list column).
 * Prefer [asExpression]/[subSelect] with an [of] prototype when used in typed operators.
 */
@Suppress("UNCHECKED_CAST")
fun OneColumnSelect<*>.asExpression(): SelectExpression<Any, SqlType> {
	val col = column()
	return SelectExpression(this, col.javaClass as Class<Any>, col.sqlType as SqlType)
}

@Suppress("UNCHECKED_CAST")
fun subSelect(select: OneColumnSelect<*>): SelectExpression<Any, SqlType> {
	val col = select.column()
	return SelectExpression(select, col.javaClass as Class<Any>, col.sqlType as SqlType)
}
