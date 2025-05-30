/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import java.sql.PreparedStatement
import java.sql.ResultSet


typealias javaToSqlFn = (Any?) -> String
typealias javaToPrepSqlFn = (PreparedStatement, Int, Any?) -> Unit
typealias sqlToJavaFn = (ResultSet, Int) -> Any?

object SqlTypeConverter {

	private var javaToSql = hashMapOf<Class<*>, HashMap<Class<*>, javaToSqlFn>>()
	private var javaToPrepSql = hashMapOf<Class<*>, HashMap<Class<*>, javaToPrepSqlFn>>()
	private var sqlToJava = hashMapOf<Class<*>, HashMap<Class<*>, sqlToJavaFn>>()

	fun registerJavaToSql(from: Class<out Any>, to: Class<out SqlType>, fn: javaToSqlFn) {
		val javaToSql = this.javaToSql
		if (!javaToSql.containsKey(from))
			javaToSql[from] = hashMapOf()
		val sql = javaToSql[from]!!
		if (sql.containsKey(to))
			throw RuntimeException("Already registered")
		sql[to] = fn
	}

	fun registerJavaToPrepSql(from: Class<out Any>, to: Class<out SqlType>, fn: javaToPrepSqlFn) {
		val javaToPrepSql = this.javaToPrepSql
		if (!javaToPrepSql.containsKey(from))
			javaToPrepSql[from] = hashMapOf()
		val sql = javaToPrepSql[from]!!
		if (sql.containsKey(to))
			throw RuntimeException("Already registered")
		sql[to] = fn
	}

	fun registerSqlToJava(from: Class<out SqlType>, to: Class<out Any>, fn: sqlToJavaFn) {
		val sqlToJava = this.sqlToJava
		if (!sqlToJava.containsKey(from))
			sqlToJava[from] = hashMapOf()
		val java = sqlToJava[from]!!
		if (java.containsKey(to))
			throw RuntimeException("Already registered")
		java[to] = fn
	}

	fun javaToSql(from: Class<out Any>, to: Class<out SqlType>, value: Any?): String {
		val j = javaToSql[from] ?: throw RuntimeException(converterIsNotDefined(from, to))
		val s = j[to] ?: throw RuntimeException(converterIsNotDefined(from, to))
		return s.invoke(value)
	}

	fun javaToPrepSql(from: Class<out Any>, to: Class<out SqlType>, ps: PreparedStatement, ind: Int, value: Any?) {
		val j = javaToPrepSql[from] ?: throw RuntimeException(converterIsNotDefined(from, to))
		val s = j[to] ?: throw RuntimeException(converterIsNotDefined(from, to))
		return s.invoke(ps, ind, value)
	}

	fun sqlToJava(from: Class<out SqlType>, to: Class<out Any>, rs: ResultSet, ind: Int): Any? {
		val s = sqlToJava[from] ?: throw RuntimeException(converterIsNotDefined(from, to))
		val j = s[to] ?: throw RuntimeException(converterIsNotDefined(from, to))
		return j.invoke(rs, ind)
	}

	fun converterIsNotDefined(from: Class<*>, to: Class<*>) = "Converter '${from.name}' -> ${to.name} isn't defined"
}
