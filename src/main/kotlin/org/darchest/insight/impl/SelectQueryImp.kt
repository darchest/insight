/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.impl

import org.darchest.insight.*
import org.darchest.insight.part.SqlPartsArray
import java.sql.Connection

/**
 * Compositional SELECT builder: emits SQL only, with no JDBC execution.
 * Used as the shared SQL core for [ReadableSelectImp] and as the basis for
 * [org.darchest.insight.expression.SelectExpression] / [org.darchest.insight.SelectDataSource].
 */
open class SelectQueryImp<T : SqlDataSource>(val source: T) : Select<T>, OneColumnSelect<T> {

	protected val groupBy = SqlPartsArray<SqlValue<*, *>>()
	protected val fields = SqlPartsArray<SqlValue<*, *>>()
	protected var whereExpr: SqlValue<*, *>? = null
	protected var havingExpr: SqlValue<*, *>? = null
	protected val orderBy = SqlPartsArray<SortInfo>()
	protected var limit: Long? = null
	protected var offset: Long? = null

	/** Fields that were last written into the SELECT list (groupBy + fields). */
	val factFields = SqlPartsArray<SqlValue<*, *>>()

	override var connection: Connection? = null

	override fun source() = source

	override fun groupBy(vararg fields: SqlValue<*, *>): Select<T> {
		this.groupBy.clear()
		this.groupBy.addAll(fields)
		return this
	}

	override fun groupBy(fields: Collection<SqlValue<*, *>>): Select<T> {
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

	override fun column(): SqlValue<*, *> {
		val selected = fieldsToSelect()
		if (selected.size != 1) {
			throw IllegalStateException(
				"OneColumnSelect requires exactly one field in the select list, got ${selected.size}"
			)
		}
		return selected.first()
	}

	fun selectListFields(): Collection<SqlValue<*, *>> = fieldsToSelect()

	fun collectReferencedValues(out: MutableCollection<SqlValue<*, *>>) {
		out.addAll(necessaryValues())
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		factFields.clear()
		factFields.addAll(fieldsToSelect())

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
		writeGroupBy(builder, vendor, groupBy, params)
		writeHaving(builder, vendor, havingExpr, params)
		writeOrder(builder, vendor, orderBy, params)
		writeLimit(builder, vendor, limit)
		writeOffset(builder, vendor, offset)
	}

	protected fun fieldsToSelect(): Collection<SqlValue<*, *>> {
		val values = mutableListOf<SqlValue<*, *>>()
		values.addAll(groupBy)
		values.addAll(fields)
		return values
	}

	protected fun necessaryValues(): Collection<SqlValue<*, *>> {
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

	protected fun allColumnsInValues(values: Collection<SqlValue<*, *>>): Collection<SqlValue<*, *>> {
		val columns = mutableListOf<SqlValue<*, *>>()
		values.forEach { v -> v.innerColumns(columns) }
		columns.forEach { c -> c.innerColumnsGetted = false }
		return columns
	}

	protected fun allSourcesByColumns(columns: Collection<SqlValue<*, *>>): MutableList<SqlDataSource> {
		val sources = mutableListOf<SqlDataSource>(source)
		for (col in columns) {
			when (col) {
				is TableColumn -> {
					val src = col.owner
					if (src != null && !sources.contains(src))
						sources.add(src)
				}
				is SelectColumn -> {
					val src = col.owner
					if (!sources.contains(src))
						sources.add(src)
				}
			}
		}
		return sources
	}

	protected fun prepareJoins(sources: MutableList<SqlDataSource>): Collection<Join<*>> {
		closeJoinSources(sources)
		if (sources.size == 1)
			return mutableListOf()
		for ((i, src) in sources.withIndex()) {
			src.sqlPseudo = "T$i"
		}
		val joins = sources.mapNotNull { it.joined }
		return orderJoins(joins, sources.toSet())
	}

	protected fun closeJoinSources(sources: MutableList<SqlDataSource>) {
		var i = 0
		while (i < sources.size) {
			val join = sources[i].joined
			if (join != null) {
				addSourceIfAbsent(sources, join.owner)
				val refs = mutableSetOf<SqlDataSource>()
				join.getExpr()?.collectReferencedSources(refs)
				refs.forEach { addSourceIfAbsent(sources, it) }
			}
			i++
		}
	}

	protected fun addSourceIfAbsent(sources: MutableList<SqlDataSource>, src: SqlDataSource) {
		if (!sources.contains(src))
			sources.add(src)
	}

	protected fun orderJoins(joins: List<Join<*>>, sourceSet: Set<SqlDataSource>): List<Join<*>> {
		fun deps(join: Join<*>): Set<SqlDataSource> {
			val d = mutableSetOf<SqlDataSource>()
			d.add(join.owner)
			join.getExpr()?.collectReferencedSources(d)
			val joined = join()
			d.remove(joined)
			val unknown = d.filter { it !in sourceSet }
			if (unknown.isNotEmpty()) {
				throw IllegalStateException(
					"JOIN ${joined.sqlName} ON references tables not present in FROM: ${unknown.joinToString { it.sqlName }}"
				)
			}
			return d
		}

		val available = mutableSetOf<SqlDataSource>(source)
		val remaining = joins.toMutableList()
		val result = mutableListOf<Join<*>>()
		while (remaining.isNotEmpty()) {
			val idx = remaining.indexOfFirst { deps(it).all { dep -> dep in available } }
			if (idx < 0) {
				val names = remaining.joinToString { j ->
					val t = j()
					"${t.sqlName}(${t.sqlPseudo})"
				}
				throw IllegalStateException("Cannot order JOINs (cyclic or unresolved dependency): $names")
			}
			val join = remaining.removeAt(idx)
			result.add(join)
			available.add(join())
		}
		return result
	}

	protected suspend fun writeJoins(builder: StringBuilder, vendor: Vendor, joins: Collection<Join<*>>, params: MutableList<SqlValue<*, *>>) {
		for (join in joins) {
			builder.append("\n\t")
			join.writeSql(builder, vendor, params)
		}
	}

	protected suspend fun writeWhere(builder: StringBuilder, vendor: Vendor, where: SqlValue<*, *>?, params: MutableList<SqlValue<*, *>>) {
		if (where == null)
			return
		if (!vendor.isSqlBoolean(where))
			throw RuntimeException("Where expr result isn't boolean")
		builder.append("\nWHERE ")
		where.writeSql(builder, vendor, params)
	}

	protected suspend fun writeGroupBy(builder: StringBuilder, vendor: Vendor, groupBy: SqlPartsArray<SqlValue<*, *>>, params: MutableList<SqlValue<*, *>>) {
		if (groupBy.isEmpty())
			return
		builder.append("\nGROUP BY ")
		groupBy.writeSql(builder, vendor, params)
	}

	protected suspend fun writeHaving(builder: StringBuilder, vendor: Vendor, having: SqlValue<*, *>?, params: MutableList<SqlValue<*, *>>) {
		if (having == null)
			return
		if (!vendor.isSqlBoolean(having))
			throw RuntimeException("Having expr result isn't boolean")
		builder.append("\nHAVING ")
		having.writeSql(builder, vendor, params)
	}

	protected suspend fun writeOrder(builder: StringBuilder, vendor: Vendor, order: SqlPartsArray<SortInfo>, params: MutableList<SqlValue<*, *>>) {
		if (order.isEmpty())
			return
		builder.append("\nORDER BY ")
		order.writeSql(builder, vendor, params)
	}

	protected fun writeLimit(builder: StringBuilder, vendor: Vendor, limit: Long?) {
		if (limit == null)
			return
		builder.append("\nLIMIT ")
		builder.append(limit)
	}

	protected fun writeOffset(builder: StringBuilder, vendor: Vendor, offset: Long?) {
		if (offset == null)
			return
		builder.append("\nOFFSET ")
		builder.append(offset)
	}
}

suspend fun <T : SqlDataSource> selectQuery(source: T, f: suspend SelectQueryImp<T>.(t: T) -> Unit = {}): SelectQueryImp<T> {
	val sel = SelectQueryImp(source)
	f.invoke(sel, source)
	return sel
}

suspend fun <T : SqlDataSource> oneColumnSelect(source: T, f: suspend SelectQueryImp<T>.(t: T) -> Unit = {}): OneColumnSelect<T> {
	val sel = selectQuery(source, f)
	sel.column() // validate exactly one select-list field
	return sel
}
