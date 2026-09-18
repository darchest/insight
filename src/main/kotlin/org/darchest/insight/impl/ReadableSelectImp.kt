/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.impl

import mu.KotlinLogging
import org.darchest.insight.*
import java.sql.Connection
import java.sql.ResultSet

/**
 * Executable SELECT: compositional SQL from [SelectQueryImp] plus JDBC read/next/close.
 */
class ReadableSelectImp<T : SqlDataSource>(source: T) : ReadableSelect<T> {

	private val logger = KotlinLogging.logger {}
	private val query = SelectQueryImp(source)

	override fun source() = query.source()

	override var connection: Connection?
		get() = query.connection
		set(value) {
			query.connection = value
		}

	override fun groupBy(vararg fields: SqlValue<*, *>): Select<T> {
		query.groupBy(*fields)
		return this
	}

	override fun groupBy(fields: Collection<SqlValue<*, *>>): Select<T> {
		query.groupBy(fields)
		return this
	}

	override fun fields(vararg fields: SqlValue<*, *>): Select<T> {
		query.fields(*fields)
		return this
	}

	override fun fields(fields: Collection<SqlValue<*, *>>): Select<T> {
		query.fields(fields)
		return this
	}

	override fun joins(vararg joins: Any?): Select<T> {
		query.joins(*joins)
		return this
	}

	override fun where(expr: () -> SqlValue<*, *>?): Select<T> {
		query.where(expr)
		return this
	}

	override fun where(expr: SqlValue<*, *>?): Select<T> {
		query.where(expr)
		return this
	}

	override fun addWhere(expr: SqlValue<*, *>): Select<T> {
		query.addWhere(expr)
		return this
	}

	override fun having(expr: () -> SqlValue<*, *>?): Select<T> {
		query.having(expr)
		return this
	}

	override fun having(expr: SqlValue<*, *>?): Select<T> {
		query.having(expr)
		return this
	}

	override fun sort(vararg sorts: SortInfo): Select<T> {
		query.sort(*sorts)
		return this
	}

	override fun sort() = query.sort()

	override fun limit(limit: Long?): Select<T> {
		query.limit(limit)
		return this
	}

	override fun offset(offset: Long?): Select<T> {
		query.offset(offset)
		return this
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		query.writeSql(builder, vendor, params)
	}

	private var resultSet: ResultSet? = null

	override suspend fun read() {
		val vendor = ConnectionManager.getVendor(source().connectionName)
		val sqlTimeFrom = System.currentTimeMillis()
		val (sql, params) = getSql(vendor)
		logger.debug { "Sql created in ${System.currentTimeMillis() - sqlTimeFrom } mills:\n${sql}" }
		val queryTimeFrom = System.currentTimeMillis()
		val connection = this.connection ?: ConnectionManager.getConnection(source().connectionName)
		val statement = connection.prepareStatement(sql)
		var ind = 0
		params.forEach {
			SqlTypeConvertersRegistry.javaToPrepSql(it.javaClass, it.sqlClass, statement, ++ind, it.getValue())
		}
		this.connection = connection
		logger.trace { "Prepared statement SELECT:\n${statement}" }
		val rs = statement.executeQuery()
		logger.debug { "SELECT query time: ${System.currentTimeMillis() - queryTimeFrom} mills" }
		resultSet = rs
		for ((fieldInd, f) in query.factFields.withIndex())
			f.setResultSetData(rs, fieldInd + 1)
	}

	override fun next(): Boolean {
		val rs = resultSet ?: throw RuntimeException("Result set is null")
		val readed = rs.next()
		if (!readed) {
			val conn = connection
			if (conn != null)
				connection = null
			resultSet = null
			query.factFields.forEach { f -> f.state = SqlValue.State.NOT_SET }
		}
		return readed
	}

	override fun close() {
	}
}

suspend fun <T : SqlDataSource> select(source: T, f: suspend ReadableSelect<T>.(t: T) -> Unit = {}): ReadableSelect<T> {
	val sel = ReadableSelectImp(source)
	f.invoke(sel, source)
	return sel
}
