/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class Constraint: SqlPrintable {

}

class PrimaryKey(vararg val columns: TableColumn<*, *>): Constraint() {
    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        builder.append("PRIMARY KEY (")
        builder.append(columns.joinToString(", ") { c -> c.name })
        builder.append(")")
    }
}

class Unique(vararg val columns: TableColumn<*, *>): Constraint() {
    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        builder.append("UNIQUE (")
        builder.append(columns.joinToString(", ") { c -> c.name })
        builder.append(")")
    }
}

class ForeignKey<T: Table>(val columns: List<TableColumn<*, *>>, foreignTableFn: () -> T, foreignColumnsFn: T.() -> List<TableColumn<*, *>>): Constraint() {

    val foreignTable: T = foreignTableFn()
    val foreignColumns: List<TableColumn<*, *>> = foreignColumnsFn(foreignTable)

    constructor(column: TableColumn<*, *>, foreignTableFn: () -> T, foreignColumnFn: T.() -> TableColumn<*, *>) : this(listOf(column), foreignTableFn, { listOf(foreignColumnFn()) })

    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        builder.append("FOREIGN KEY (")
        builder.append(columns.joinToString(", ") { c -> c.name })
        builder.append(") REFERENCES ")
        builder.append(foreignTable.sqlName)
        builder.append(" (")
        builder.append(foreignColumns.joinToString(", ") { c -> c.name })
        builder.append(")")
    }
}