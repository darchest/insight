/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import org.darchest.insight.impl.SelectQueryImp

/**
 * Derived table: a [Select] used as [SqlDataSource] in FROM / JOIN.
 * Emits `(SELECT …) alias` and projects inner select-list fields as [SelectColumn]s.
 */
class SelectDataSource(
	val select: Select<*>,
	alias: String
) : SqlDataSource(alias) {

	private val selectColumns = mutableListOf<SelectColumn<*, *>>()

	init {
		val query = select as? SelectQueryImp<*>
			?: throw IllegalArgumentException(
				"SelectDataSource requires a compositional SelectQueryImp (use selectQuery { … }), got ${select::class.simpleName}"
			)
		for ((index, field) in query.selectListFields().withIndex()) {
			@Suppress("UNCHECKED_CAST")
			registerProjectedField(field as SqlValue<Any, SqlType>, index)
		}
	}

	private fun <J : Any, S : SqlType> registerProjectedField(field: SqlValue<J, S>, index: Int) {
		val name = when (field) {
			is TableColumn -> field.name
			else -> field.codeName.ifEmpty { "col$index" }
		}
		SelectColumn(name, field.javaClass, field.sqlType, this, field)
	}

	internal fun registerSelectColumn(column: SelectColumn<*, *>) {
		if (selectColumns.any { it.name == column.name }) {
			throw RuntimeException("Column '${column.name}' already registered in select data source '$sqlName'")
		}
		selectColumns.add(column)
		sqlByNames[column.name] = column
		namesBySql[column] = column.name
	}

	fun columns(): List<SelectColumn<*, *>> = selectColumns.toList()

	fun column(name: String): SelectColumn<*, *> =
		selectColumns.first { it.name == name }

	@Suppress("UNCHECKED_CAST")
	fun <J : Any, S : SqlType> column(sourceField: SqlValue<J, S>): SelectColumn<J, S> {
		val found = selectColumns.firstOrNull { it.sourceField === sourceField }
			?: throw NoSuchElementException("No projected column for the given source field")
		return found as SelectColumn<J, S>
	}

	override val connectionName: String?
		get() = select.source().connectionName

	override fun vendor(): Vendor = select.source().vendor()

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("(")
		select.writeSql(builder, vendor, params)
		builder.append(") ")
		builder.append(sqlPseudo ?: sqlName)
	}
}

/** Wrap this compositional select as a derived-table [SqlDataSource]. */
fun Select<*>.asDataSource(alias: String): SelectDataSource = SelectDataSource(this, alias)
