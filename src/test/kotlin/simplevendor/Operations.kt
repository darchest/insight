/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.ComparisonOperation
import org.darchest.insight.LogicalOperation
import org.darchest.insight.SqlConst
import org.darchest.insight.SqlValue
import java.time.Instant
import java.util.*

infix fun<sqlT: PostgresqlType> SqlValue<*, out sqlT>.eq(right: SqlValue<*, out sqlT>) = PostgresqlComparison(this, ComparisonOperation.Operator.EQ, right)
infix fun<sqlT: PostgresqlType> SqlValue<*, out sqlT>.neq(right: SqlValue<*, out sqlT>) = PostgresqlComparison(this, ComparisonOperation.Operator.NEQ, right)

infix fun<sqlT: PostgresqlType> SqlValue<*, out sqlT>.gt(right: SqlValue<*, out sqlT>) = PostgresqlComparison(this, ComparisonOperation.Operator.GT, right)
infix fun<sqlT: PostgresqlType> SqlValue<*, out sqlT>.ge(right: SqlValue<*, out sqlT>) = PostgresqlComparison(this, ComparisonOperation.Operator.GE, right)

infix fun<sqlT: PostgresqlType> SqlValue<*, out sqlT>.lt(right: SqlValue<*, out sqlT>) = PostgresqlComparison(this, ComparisonOperation.Operator.LT, right)
infix fun<sqlT: PostgresqlType> SqlValue<*, out sqlT>.le(right: SqlValue<*, out sqlT>) = PostgresqlComparison(this, ComparisonOperation.Operator.LE, right)


inline infix fun<reified J: Any, reified sqlT: PostgresqlType> SqlValue<J, out sqlT>.eq(right: J?) = PostgresqlComparison(this, ComparisonOperation.Operator.EQ, SqlConst(right, J::class.java, this.sqlType))
inline infix fun<reified J: Any, reified sqlT: PostgresqlType> SqlValue<J, out sqlT>.neq(right: J?) = PostgresqlComparison(this, ComparisonOperation.Operator.NEQ, SqlConst(right, J::class.java, this.sqlType))

inline infix fun<reified J: Any, reified sqlT: PostgresqlType> SqlValue<J, out sqlT>.gt(right: J?) = PostgresqlComparison(this, ComparisonOperation.Operator.GT, SqlConst(right, J::class.java, this.sqlType))
inline infix fun<reified J: Any, reified sqlT: PostgresqlType> SqlValue<J, out sqlT>.ge(right: J?) = PostgresqlComparison(this, ComparisonOperation.Operator.GE, SqlConst(right, J::class.java, this.sqlType))

inline infix fun<reified J: Any, reified sqlT: PostgresqlType> SqlValue<J, out sqlT>.lt(right: J?) = PostgresqlComparison(this, ComparisonOperation.Operator.LT, SqlConst(right, J::class.java, this.sqlType))
inline infix fun<reified J: Any, reified sqlT: PostgresqlType> SqlValue<J, out sqlT>.le(right: J?) = PostgresqlComparison(this, ComparisonOperation.Operator.LE, SqlConst(right, J::class.java, this.sqlType))


inline infix fun SqlValue<Instant, *>.eq(right: Long) = PostgresqlComparison(this, ComparisonOperation.Operator.EQ, SqlConst(right, Long::class.java, this.sqlType))
inline infix fun SqlValue<Instant, *>.neq(right: Long) = PostgresqlComparison(this, ComparisonOperation.Operator.NEQ, SqlConst(right, Long::class.java, this.sqlType))

infix fun SqlValue<*, out BooleanType>.and(right: SqlValue<*, out BooleanType>) = PostgresqlLogical(LogicalOperation.Operator.AND, listOf(this, right))
infix fun SqlValue<*, out BooleanType>.or(right: SqlValue<*, out BooleanType>) = PostgresqlLogical(LogicalOperation.Operator.OR, listOf(this, right))

fun and(vararg values: SqlValue<*, out BooleanType>) = PostgresqlLogical(LogicalOperation.Operator.AND, values.toList())
fun or(vararg values: SqlValue<*, out BooleanType>) = PostgresqlLogical(LogicalOperation.Operator.OR, values.toList())

fun and(values: List<SqlValue<*, out BooleanType>>) = PostgresqlLogical(LogicalOperation.Operator.AND, values)
fun or(values: List<SqlValue<*, out BooleanType>>) = PostgresqlLogical(LogicalOperation.Operator.OR, values)

fun<sqlT: PostgresqlType> SqlValue<*, out sqlT>.notIn(arr: List<SqlValue<*, out sqlT>>) = PostgresInOperator(this, arr, true)

fun <SqlT: PostgresqlType> SqlValue<*, out PostgresqlType>.castTo(type: SqlT): SqlValue<*, SqlT> = PostgresqlCast(this, type)

fun SqlValue<UUIDArray, UuidArrayType>.include(vararg elems: UUID) = PostgresArrayContains(this, SqlConst(UUIDArray().apply { addAll(elems) }, UUIDArray::class.java, UuidArrayType()))