/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.impl

import mu.KotlinLogging
import org.darchest.insight.*
import org.darchest.insight.part.SqlPartsArray
import java.sql.Connection
import java.sql.ResultSet

class ReadableSelectImp<T: SqlDataSource>(val source: T): ReadableSelect<T> {

	private val logger = KotlinLogging.logger {}

	private val groupBy = SqlPartsArray<SqlValue<*, *>>()
	private val fields = SqlPartsArray<SqlValue<*, *>>()
	private var whereExpr: SqlValue<*, *>? = null
	private var havingExpr: SqlValue<*, *>? = null
	private val orderBy = SqlPartsArray<SortInfo>()
	private var limit: Long? = null
	private var offset: Long? = null

	private val factFields = SqlPartsArray<SqlValue<*, *>>()

	override fun source() = source

	override fun groupBy(vararg fields: SqlValue<*, *>): Select<T> {
		this.groupBy.clear()
		this.groupBy.addAll(fields)
		return this
	}

	override fun fields(vararg fields: SqlValue<*, *>): Select<T> {
		return fields(fields.toList())
	}

	override fun fields(fields: Collection<SqlValue<*, *>>): Select<T> {
		this.fields.clear()
		this.fields.addAll(fields)
		return this
	}

	override fun joins(vararg joins: Any?): Select<T> {
		TODO("Not yet implemented")
	}

	override fun where(expr: () -> SqlValue<*, *>?): Select<T> {
		whereExpr = expr.invoke()
		return this
	}

	override fun where(expr: SqlValue<*, *>?): Select<T> {
		whereExpr = expr
		return this
	}

	override fun addWhere(expr: SqlValue<*, *>): Select<T> {
		var currentWhere = whereExpr
		if (currentWhere == null) {
			currentWhere = expr
		} else {
			currentWhere = source.vendor().createLogicalOperation(LogicalOperation.Operator.AND, listOf(currentWhere, expr))
		}
		whereExpr = currentWhere
		return this
	}

	override fun having(expr: () -> SqlValue<*, *>?): Select<T> {
		havingExpr = expr.invoke()
		return this
	}

	override fun having(expr: SqlValue<*, *>?): Select<T> {
		havingExpr = expr
		return this
	}

	override fun sort(vararg sorts: SortInfo): Select<T> {
		orderBy.clear()
		orderBy.addAll(sorts)
		return this
	}

	override fun sort() = orderBy

	override fun limit(limit: Long?): Select<T> {
		this.limit = limit
		return this
	}

	override fun offset(offset: Long?): Select<T> {
		this.offset = offset
		return this
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		val toSelect = fieldsToSelect()
		factFields.addAll(allColumnsInValues(toSelect))

		val allUsed = necessaryValues()
		val allRealUsed = allColumnsInValues(allUsed)
		val sources = allSourcesByColumns(allRealUsed)
		val joins = prepareJoins(sources)

		builder.append("SELECT ")
		factFields.writeSql(builder, vendor, params)
		builder.append("\n")
		builder.append("FROM ")
		source.writeSql(builder, vendor, params)

		writeJoins(builder, vendor, joins, params)
		writeWhere(builder, vendor, whereExpr, params)
		writeOrder(builder, vendor, orderBy, params)
		writeLimit(builder, vendor, limit)
		writeOffset(builder, vendor, offset)
	}

	private fun fieldsToSelect(): Collection<SqlValue<*, *>> {
		val values = mutableListOf<SqlValue<*, *>>()
		values.addAll(groupBy)
		values.addAll(fields)
		return values
	}

	private fun necessaryValues(): Collection<SqlValue<*, *>> {
		val values = mutableListOf<SqlValue<*, *>>()
		values.addAll(groupBy)
		values.addAll(fields)
		val where = whereExpr
		if (where != null)
			values.add(where)
		val having = havingExpr
		if (having != null)
			values.add(having)
		orderBy.forEach { values.add(it.expr) }
		return values
	}

	private fun allColumnsInValues(values: Collection<SqlValue<*, *>>): Collection<SqlValue<*, *>> {
		val columns = mutableListOf<SqlValue<*, *>>()
		values.forEach { v -> v.innerColumns(columns) }
		columns.forEach { c -> c.innerColumnsGetted = false }
		return columns
	}

	private fun allSourcesByColumns(columns: Collection<SqlValue<*, *>>): Collection<SqlDataSource> {
		val sources = mutableListOf<SqlDataSource>(source)
		for (col in columns) {
			if (col is TableColumn) {
				val src = col.owner
				if (src != null && !sources.contains(src))
					sources.add(src)
			}
		}
		return sources
	}

	private fun prepareJoins(sources: Collection<SqlDataSource>): Collection<Join<*>> {
		if (sources.size == 1)
			return mutableListOf()
		val joins = mutableListOf<Join<*>>()
		for ((i, src) in sources.withIndex()) {
			src.sqlPseudo = "T$i"
			val joined = src.joined
			if (joined != null)
				joins.add(joined)
		}
		return joins
	}

	private suspend fun writeJoins(builder: StringBuilder, vendor: Vendor, joins: Collection<Join<*>>, params: MutableList<SqlValue<*, *>>) {
		for (join in joins) {
			builder.append("\n\t")
			join.writeSql(builder, vendor, params)
		}
	}

	private suspend fun writeWhere(builder: StringBuilder, vendor: Vendor, where: SqlValue<*, *>?, params: MutableList<SqlValue<*, *>>) {
		if (where == null)
			return
		if (!vendor.isSqlBoolean(where))
			throw RuntimeException("Where expr result isn't boolean")
		builder.append("\nWHERE ")
		where.writeSql(builder, vendor, params)
	}

	private suspend fun writeOrder(builder: StringBuilder, vendor: Vendor, order: SqlPartsArray<SortInfo>, params: MutableList<SqlValue<*, *>>) {
		if (order.isEmpty())
			return
		builder.append("\nORDER BY ")
		order.writeSql(builder, vendor, params)
	}

	private fun writeLimit(builder: StringBuilder, vendor: Vendor, limit: Long?) {
		if (limit == null)
			return
		builder.append("\nLIMIT ")
		builder.append(limit)
	}

	private fun writeOffset(builder: StringBuilder, vendor: Vendor, offset: Long?) {
		if (offset == null)
			return
		builder.append("\nOFFSET ")
		builder.append(offset)
	}

	override var connection: Connection? = null
	private var resultSet: ResultSet? = null

	override suspend fun read() {
		val vendor = ConnectionManager.getVendor(source.connectionName)
		val sqlTimeFrom = System.currentTimeMillis()
		val (sql, params) = getSql(vendor)
		logger.debug { "Sql created in ${System.currentTimeMillis() - sqlTimeFrom } mills:\n${sql}" }
		val queryTimeFrom = System.currentTimeMillis()
		val connection = this.connection?: ConnectionManager.getConnection(source.connectionName)
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
		for ((ind, f) in factFields.withIndex())
			f.setResultSetData(rs, ind + 1)
	}

	override fun next(): Boolean {
		val rs = resultSet ?: throw RuntimeException("Result set is null")
		val readed = rs.next()
		if (!readed) {
			val conn = connection
			if (conn != null)
				connection = null
			resultSet = null
			fields.forEach { f -> f.state = SqlValue.State.NOT_SET }
		}
		return readed
	}

	override fun close() {

	}
}

suspend fun <T: SqlDataSource> select(source: T, f: suspend ReadableSelect<T>.(t: T) -> Unit = {}): ReadableSelect<T>   {
	val sel = ReadableSelectImp(source)
	f.invoke(sel, source)
	return sel
}