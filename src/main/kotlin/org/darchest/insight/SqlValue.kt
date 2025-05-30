/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import java.sql.ResultSet

abstract class SqlValue<javaType: Any, sqlT: SqlType>(val javaClass: Class<javaType>, val sqlType: sqlT): SqlPrintable {

	val sqlClass: Class<out sqlT> = sqlType::class.java

	var sqlDataSource: SqlDataSource? = null
	var codeName = ""

	fun fullCodeName(): String {
		val source = sqlDataSource
		if (source == null)
			return codeName
		val join = source.joined
		if (join == null)
			return codeName
		return join.codeName + codeName
	}

	enum class State { NOT_SET, READED, VALUE_SET, EXPR_SET }

	open var state = State.NOT_SET

	@Suppress("UNCHECKED_CAST")
	open suspend fun getValue(): javaType? {
		if (state == State.NOT_SET)
			throw RuntimeException("Value isn't set")
		return SqlTypeConverter.sqlToJava(sqlClass, javaClass, resultSet!!, resultSetInd!!) as? javaType?
	}

	suspend fun getAsAny(): Any? = getValue()

	fun asc() = SortInfo(this, SortInfo.Direction.ASC)
	fun desc() = SortInfo(this, SortInfo.Direction.DESC)

	var resultSet: ResultSet? = null
	var resultSetInd: Int? = null

	fun setResultSetData(rs: ResultSet?, ind: Int?) {
		resultSet = rs
		resultSetInd = ind
		state = State.READED
	}

	var innerColumnsGetted = false

	fun innerColumns(array: MutableCollection<SqlValue<*, *>>) {
		if (!innerColumnsGetted) {
			innerColumnsGetted = true
			fillByInnerColumns(array)
		}
	}

	open fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) { }
}