/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

interface Vendor {
	fun init()

	fun isSqlBoolean(value: SqlValue<*, *>) = isBoolean(value.sqlClass)
	fun isBoolean(type: Class<out SqlType>): Boolean

	fun getTables(dataSourceName: String)

	fun writeSqlDataSource(dataSource: SqlDataSource, builder: StringBuilder)
	fun writeSqlColumnName(column: TableColumn<*, *>, builder: StringBuilder)

	fun createLogicalOperation(op: LogicalOperation.Operator, values: Collection<SqlValue<*, *>>): LogicalOperation<*, *>

	fun createComparisonOperation(left: SqlValue<*, *>, op: ComparisonOperation.Operator, right: SqlValue<*, *>): ComparisonOperation<*, *>

	fun getCountExpression(): Expression<Long, *>
}