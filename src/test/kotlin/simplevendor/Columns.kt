/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.*
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*

open class PostgresqlColumn<javaType: Any, sqlT: PostgresqlType>(
	name: String,
	javaClass: Class<javaType>,
	sqlType: sqlT,
	defaultValue: javaType,
	length: Int? = null
): TableColumn<javaType, sqlT>(
	name,
	javaClass,
	sqlType,
	length,
	{ SqlConst(defaultValue, javaClass, sqlType) }
)

class PostgresqlComparison(left: SqlValue<*, *>, operator: Operator, right: SqlValue<*, *>): ComparisonOperation<Boolean, BooleanType>(left, operator, right, Boolean::class.java, BooleanType())

class PostgresqlLogical(operator: Operator, values: Collection<SqlValue<*, *>>): LogicalOperation<Boolean, BooleanType>(operator, values, Boolean::class.java, BooleanType())


open class BoolColumn(name: String, defaultValue: Boolean = false): PostgresqlColumn<Boolean, BooleanType>(
	name,
	Boolean::class.java,
	BooleanType(),
	defaultValue
)

open class NumericColumn(name: String, defaultValue: Double = 0.0): PostgresqlColumn<Double, NumericType>(
	name,
	Double::class.java,
	NumericType("numeric"),
	defaultValue
)

open class ShortColumn(name: String, defaultValue: Short = 0): PostgresqlColumn<Short, SmallIntType>(
	name,
	Short::class.java,
	SmallIntType(),
	defaultValue
)

open class IntColumn(name: String, defaultValue: Int = 0): PostgresqlColumn<Int, IntType>(
	name,
	Int::class.java,
	IntType(),
	defaultValue
)

open class LongColumn(name: String, defaultValue: Long = 0L): PostgresqlColumn<Long, BigIntType>(
	name,
	Long::class.java,
	BigIntType(),
	defaultValue
)

open class UuidColumn(
	name: String,
	defaultValue: UUID = UUID(0, 0)
): PostgresqlColumn<UUID, UuidType>(
	name,
	UUID::class.java,
	UuidType(),
	defaultValue
)

class UUIDArray: ArrayList<UUID> {

	constructor(): super()

	constructor(arr: Collection<UUID>): super(arr)
}

open class UuidArrayColumn(name: String, defaultValue: UUIDArray = UUIDArray()): PostgresqlColumn<UUIDArray, UuidArrayType>(
	name,
	UUIDArray::class.java,
	UuidArrayType(),
	defaultValue
)

open class VarCharColumn(name: String, length: Int? = null, defaultValue: String = ""): PostgresqlColumn<String, StringType>(
	name,
	String::class.java,
	VarCharType(),
	defaultValue,
	length
)

open class ByteaTextColumn(name: String, defaultValue: String = ""): PostgresqlColumn<String, ByteaType>(
	name,
	String::class.java,
	ByteaType(),
	defaultValue
)

open class BinaryColumn(name: String, defaultValue: ByteArray = byteArrayOf()): PostgresqlColumn<ByteArray, ByteaType>(
	name,
	ByteArray::class.java,
	ByteaType(),
	defaultValue
)

abstract class PostgresqlExpression<javaType: Any, sqlT: PostgresqlType>(javaClass: Class<javaType>, sqlType: sqlT): Expression<javaType, sqlT>(javaClass, sqlType), SqlValueNotNullGetter<javaType>

abstract class PostgresqlLocalExpression<javaType: Any>(javaClass: Class<javaType>, innerColumns: List<TableColumn<*, *>>, fn: suspend () -> javaType?): LocalExpression<javaType>(javaClass, innerColumns, fn)

open class StringExpression(private val value: SqlValue<*, VarCharType>): PostgresqlExpression<String, StringType>(String::class.java, VarCharType()) {
	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		value.writeSql(builder, vendor, params)
	}
}

open class BooleanExpression(private val value: Boolean): PostgresqlExpression<Boolean, BooleanType>(Boolean::class.java, BooleanType()) {
	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append(if (value) "1" else "0")
	}
}

class CountExpression: PostgresqlExpression<Long, BigIntType>(Long::class.java, BigIntType()) {
	override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
		builder.append("COUNT(1)")
	}

	override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
		super.fillByInnerColumns(array)
		array.add(this)
	}
}