/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class SqlDataSource(val sqlName: String): SqlPrintable {
	var sqlPseudo: String? = null
	var joined: Join<*>? = null

	val sqlByNames = mutableMapOf<String, SqlValue<*, *>>()
	val namesBySql = mutableMapOf<SqlValue<*, *>, String>()

	val joinByNames = mutableMapOf<String, Join<*>>()

	open val schemaName: String? = null
	open val connectionName: String? = null

	fun codeNameOf(prop: SqlValue<*, *>): String {
		if (joined != null)
			TODO()
		return namesBySql[prop]!!
	}

	fun sqlValueByCodeName(path: String): SqlValue<*, *>? {
		var source = this
		val splited = path.split('.')
		for (i in 0..<splited.size - 1)
			source = source.joinByNames[splited[i]]!!()
		return source.sqlByNames[splited.last()]
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		schemaName?.apply { builder.append(this, ".") }
		vendor.writeSqlDataSource(this, builder)
		sqlPseudo?.apply { builder.append(" ", this) }
	}

	abstract fun vendor(): Vendor
}