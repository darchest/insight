/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

interface SqlValueGetter<javaType> {

	suspend fun getValue(): javaType?

}

interface SqlValueNotNullGetter<javaType>: SqlValueGetter<javaType> {

	suspend operator fun invoke(): javaType = getValue()!!
}

interface SqlValueNullGetter<javaType>: SqlValueGetter<javaType> {

	suspend operator fun invoke(): javaType? = getValue()
}

open class TableColumn<javaType: Any, sqlT: SqlType>(val name: String, javaClass: Class<javaType>, sqlType: sqlT, val length: Int? = null): SqlValue<javaType, sqlT>(javaClass, sqlType), SqlValueNotNullGetter<javaType> {

	var owner: Table? = null

	var settedValue: javaType? = null
	var settedExpr: SqlValue<javaType, sqlT>? = null

	fun getSqlStringType(): String = sqlType.sqlName

	@Suppress("UNCHECKED_CAST")
	override suspend fun getValue(): javaType? {
		if (state == State.NOT_SET)
			throw RuntimeException("Value $codeName isn't set")
		if (state == State.VALUE_SET)
			return settedValue
		return SqlTypeConverter.sqlToJava(sqlClass, javaClass, resultSet!!, resultSetInd!!) as? javaType?
	}

	operator fun invoke(value: javaType?) {
		if (state == State.READED)
			throw RuntimeException("Column is in read mode and can't be setted")
		settedValue = value
		state = State.VALUE_SET
	}

	open fun default(): SqlValue<*, sqlT>? = null

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		val owner = this.owner ?: throw RuntimeException("Column's owner isn't defined")
		val pseudo = owner.sqlPseudo
		if (pseudo != null) {
			builder.append(pseudo)
			builder.append(".")
		}
		vendor.writeSqlColumnName(this, builder)
	}

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		val owner = owner
		if (owner != null) {
			val joined = owner.joined
			if (joined != null)
				joined.getExpr()?.innerColumns(array)
		}
		array.add(this)
	}

}