/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

class Join<T: SqlDataSource>(val owner: SqlDataSource, private val tableFactory: () -> T): SqlPrintable {
	enum class Type {
		INNER,
		LEFT,
		RIGHT,
	}

	var codeName = ""

	var inited: Boolean = false
		private set

	private var table: T? = null

	private var type: Type = Type.INNER
	private var expr: SqlValue<*, *>? = null

	fun type(type: Type): Join<T> {
		this.type = type
		return this
	}

	fun expr(expr: () -> SqlValue<*, *>?): Join<T> {
		this.expr = expr()
		return this
	}

	fun getExpr() = expr

	operator fun invoke(): T {
		var t = table
		if (t == null) {
			t = tableFactory()
			t.joined = this
			table = t
			inited = true
		}
		return t
	}

	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append(typeToSql(type))
		builder.append(" JOIN ")
		table!!.writeSql(builder, vendor, params)
		builder.append(" ON ")
		val expr = this.expr ?: throw RuntimeException("Expr isn't set")
		if (!vendor.isSqlBoolean(expr))
			throw RuntimeException("Expr result isn't boolean")
		expr.writeSql(builder, vendor, params)
	}

	fun typeToSql(type: Type): String {
		return when (type) {
			Type.INNER -> "INNER"
			Type.LEFT -> "LEFT"
			Type.RIGHT -> "RIGHT"
		}
	}
}