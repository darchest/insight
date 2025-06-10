/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.*
import java.time.Instant
import java.util.*

open class PostgresqlColumn<javaType: Any, sqlT: PostgresqlType>(name: String, javaClass: Class<javaType>, sqlType: sqlT, length: Int? = null): TableColumn<javaType, sqlT>(name, javaClass, sqlType, length)

class PostgresqlComparison(left: SqlValue<*, *>, operator: Operator, right: SqlValue<*, *>): ComparisonOperation<Boolean, BooleanType>(left, operator, right, Boolean::class.java, BooleanType())

class PostgresqlLogical(operator: Operator, values: Collection<SqlValue<*, *>>): LogicalOperation<Boolean, BooleanType>(operator, values, Boolean::class.java, BooleanType())


open class BoolColumn(name: String): PostgresqlColumn<Boolean, BooleanType>(name, Boolean::class.java, BooleanType()) {
	override fun default(): SqlValue<*, BooleanType>? = SqlConst(false, Boolean::class.java, BooleanType())
}

open class ShortColumn(name: String): PostgresqlColumn<Short, SmallIntType>(name, Short::class.java, SmallIntType())

open class IntColumn(name: String): PostgresqlColumn<Int, IntType>(name, Int::class.java, IntType()) {
	override fun default(): SqlValue<*, IntType>? = SqlConst(0, Int::class.java, IntType())
}

open class LongColumn(name: String): PostgresqlColumn<Long, BigIntType>(name, Long::class.java, BigIntType()) {
	override fun default(): SqlValue<*, BigIntType>? = SqlConst(0L, Long::class.java, BigIntType())
}

open class UuidColumn(name: String): PostgresqlColumn<UUID, UuidType>(name, UUID::class.java, UuidType()) {
	override fun default(): SqlValue<*, UuidType>? = SqlConst(UUID.fromString("00000000-0000-0000-0000-000000000000"), UUID::class.java, UuidType())
}

class UUIDArray: ArrayList<UUID> {

	constructor(): super()

	constructor(arr: Collection<UUID>): super(arr)
}

open class UuidArrayColumn(name: String): PostgresqlColumn<UUIDArray, UuidArrayType>(name, UUIDArray::class.java, UuidArrayType()) {
	override fun default(): SqlValue<*, UuidArrayType>? = SqlConst(UUIDArray(), UUIDArray::class.java, UuidArrayType())
}

open class VarCharColumn(name: String, length: Int? = null): PostgresqlColumn<String, StringType>(name, String::class.java, VarCharType(), length) {
	override fun default(): SqlValue<*, StringType> = SqlConst("", String::class.java, VarCharType())
}

open class ByteaTextColumn(name: String): PostgresqlColumn<String, ByteaType>(name, String::class.java, ByteaType())

open class BinaryColumn(name: String): PostgresqlColumn<ByteArray, ByteaType>(name, ByteArray::class.java, ByteaType()) {
	override fun default(): SqlValue<*, ByteaType> = SqlConst("\\x", String::class.java, ByteaType())
}

open class DateColumn(name: String): PostgresqlColumn<Instant, BigIntType>(name, Instant::class.java, BigIntType()) {
	override fun default(): SqlValue<*, BigIntType>? = SqlConst(Long.MIN_VALUE, Long::class.java, BigIntType())
}

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