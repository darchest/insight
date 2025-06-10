/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class Table(sqlName: String): SqlDataSource(sqlName) {

	private val columns = arrayListOf<TableColumn<*, *>>()

	fun registerColumn(column: TableColumn<*, *>) {
		if (columns.any { it.name == column.name })
			throw RuntimeException("Column by name '${column.name}' already registered in table '$sqlName'")
		columns.add(column)
		column.owner = this
	}

	fun columns(): ArrayList<TableColumn<*, *>> {
		val cols = arrayListOf<TableColumn<*, *>>()
		cols.addAll(columns)
		return cols
	}

	fun column(name: String): TableColumn<*, *> {
		return columns.first { c -> c.name == name }
	}

	open fun constraints(): MutableList<Constraint> = mutableListOf()

	open fun indexes(): MutableList<Index> = mutableListOf()
}