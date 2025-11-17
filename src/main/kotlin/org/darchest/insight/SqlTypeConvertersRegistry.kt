/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import java.sql.PreparedStatement
import java.sql.ResultSet

interface SqlTypeConverter {

	fun javaToSql(value: Any?): String

	fun javaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any?)

	fun sqlToJava(rs: ResultSet, ind: Int): Any?
}

object SqlTypeConvertersRegistry {

	private val converters = hashMapOf<Class<*>, HashMap<Class<out SqlType>, SqlTypeConverter>>()

	fun registerConverter(from: Class<out Any>, to: Class<out SqlType>, converter: SqlTypeConverter, force: Boolean = false) {
		val javaConcrete = converters.getOrPut(from) {  HashMap() }
		if (!force && javaConcrete.containsKey(to))
			throw RuntimeException("Already registered")
		javaConcrete[to] = converter
	}

	fun javaToSql(from: Class<out Any>, to: Class<out SqlType>, value: Any?): String {
		val converter = getConverter(from, to)
		return converter.javaToSql(value)
	}

	fun javaToPrepSql(from: Class<out Any>, to: Class<out SqlType>, ps: PreparedStatement, ind: Int, value: Any?) {
		val converter = getConverter(from, to)
		return converter.javaToPreparedSql(ps,ind, value)
	}

	fun sqlToJava(from: Class<out SqlType>, to: Class<out Any>, rs: ResultSet, ind: Int): Any? {
		val converter = getConverter(to, from)
		val value = converter.sqlToJava(rs, ind)
		if (value == null || rs.wasNull())
			return null
		return value
	}

	fun getConverter(from: Class<out Any>, to: Class<out SqlType>): SqlTypeConverter {
		val s = converters[from] ?: throw RuntimeException(converterIsNotDefined(from, to))
		return s[to] ?: throw RuntimeException(converterIsNotDefined(from, to))
	}

	fun converterIsNotDefined(from: Class<*>, to: Class<*>) = "Converter '${from.name}' -> ${to.name} isn't defined"
}
