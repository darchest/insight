/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

/**
 * Column projected from a [SelectDataSource] (derived table).
 * Not a [TableColumn]: no insert/update value semantics.
 */
class SelectColumn<javaType : Any, sqlT : SqlType>(
	val name: String,
	javaClass: Class<javaType>,
	sqlType: sqlT,
	val owner: SelectDataSource,
	val sourceField: SqlValue<javaType, sqlT>
) : SqlValue<javaType, sqlT>(javaClass, sqlType), SqlValueNotNullGetter<javaType> {

	init {
		sqlDataSource = owner
		codeName = name
		owner.registerSelectColumn(this)
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		val pseudo = owner.sqlPseudo
		if (pseudo != null) {
			builder.append(pseudo)
			builder.append(".")
		}
		vendor.writeSqlSelectColumnName(this, builder)
	}

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		val joined = owner.joined
		joined?.getExpr()?.innerColumns(array)
		array.add(this)
	}

	override fun collectReferencedSources(out: MutableSet<SqlDataSource>) {
		out.add(owner)
	}
}
