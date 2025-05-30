/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import org.darchest.insight.part.SqlPartsArray
import java.sql.Connection

interface Select<T: SqlDataSource>: SqlPrintable {
	fun source(): T

	var connection: Connection?

	fun groupBy(vararg fields: SqlValue<*, *>): Select<T>
	fun fields(vararg fields: SqlValue<*, *>): Select<T>
	fun fields(fields: Collection<SqlValue<*, *>>): Select<T>
	fun joins(vararg joins: Any?): Select<T>
	fun where(expr: () -> SqlValue<*, *>?): Select<T>
	fun where(expr: SqlValue<*, *>?): Select<T>
	fun addWhere(expr: SqlValue<*, *>): Select<T>
	fun having(expr: () -> SqlValue<*, *>?): Select<T>
	fun having(expr: SqlValue<*, *>?): Select<T>
	fun sort(vararg sorts: SortInfo): Select<T>
	fun limit(limit: Long?): Select<T>
	fun offset(offset: Long?): Select<T>
    fun sort(): SqlPartsArray<SortInfo>
}