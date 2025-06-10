/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.*
import java.time.Instant
import java.util.*
import kotlin.reflect.KProperty

open class PostgresTable(name: String): Table(name) {

	override fun vendor(): Vendor = PostgresVendor

	class JoinDelegate<T: PostgresTable>(private val tableFactory: () -> T, private val where: (T) -> SqlValue<*, *>, private val type: Join.Type = Join.Type.INNER) {
		lateinit var join: Join<T>

		operator fun provideDelegate(thisRef: PostgresTable, prop: KProperty<*>): JoinDelegate<T> {
			join = Join(thisRef, tableFactory)
			join.codeName = (if (thisRef.joined != null) thisRef.joined!!.codeName + prop.name else prop.name) + "."
			thisRef.joinByNames[prop.name] = join
			return this
		}

		operator fun getValue(thisRef: PostgresTable, prop: KProperty<*>): Join<T> {
			if (!join.inited) {
				join.codeName = (if (thisRef.joined != null) thisRef.joined!!.codeName + prop.name else prop.name) + "."
				val t = join()
				join.expr { where.invoke(t) }
				join.type(type)
			}
			return join
		}
	}

	open class ColDelegate<T: PostgresqlColumn<*, *>>(private val col: T) {

		operator fun provideDelegate(thisRef: PostgresTable, prop: KProperty<*>): ColDelegate<T> {
			thisRef.registerColumn(col)
			col.sqlDataSource = thisRef
			col.codeName = if (thisRef.joined != null) thisRef.joined!!.codeName + prop.name else prop.name
			thisRef.sqlByNames[prop.name] = col
			thisRef.namesBySql[col] = prop.name
			return this
		}

		operator fun getValue(thisRef: PostgresTable, property: KProperty<*>): T {
			return col
		}
	}

	class UUIDCol(name: String): ColDelegate<UuidColumn>(UuidColumn(name))

	class UUIDArrayCol(name: String): ColDelegate<UuidArrayColumn>(UuidArrayColumn(name))


	class VarCharCol(name: String, length: Int? = null): ColDelegate<VarCharColumn>(VarCharColumn(name, length))

	class ByteaTextCol(name: String): ColDelegate<ByteaTextColumn>(ByteaTextColumn(name))

	class BinaryCol(name: String): ColDelegate<BinaryColumn>(BinaryColumn(name))

	class ShortCol(name: String): ColDelegate<ShortColumn>(ShortColumn(name))

	class IntCol(name: String): ColDelegate<IntColumn>(IntColumn(name))

	class LongCol(name: String): ColDelegate<LongColumn>(LongColumn(name))

	class BoolCol(name: String): ColDelegate<BoolColumn>(BoolColumn(name))

	class DateCol(name: String): ColDelegate<DateColumn>(DateColumn(name))

	fun <T: PostgresTable> countExpr() = CountExpression()

	open class ExprDelegate<T: PostgresqlExpression<*, *>>(private val expr: T) {

		operator fun provideDelegate(thisRef: PostgresTable, prop: KProperty<*>): ExprDelegate<T> {
			expr.sqlDataSource = thisRef
			expr.codeName = if (thisRef.joined != null) thisRef.joined!!.codeName + prop.name else prop.name
			thisRef.sqlByNames[prop.name] = expr
			thisRef.namesBySql[expr] = prop.name
			return this
		}

		operator fun getValue(thisRef: PostgresTable, property: KProperty<*>): T {
			return expr
		}
	}

	class StringExpr(value: SqlValue<*, VarCharType>): ExprDelegate<StringExpression>(StringExpression(value))

	class BoolExpr(value: Boolean): ExprDelegate<BooleanExpression>(BooleanExpression(value))

	class StringLocalExpression(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> String?): PostgresqlLocalExpression<String>(String::class.java, innerColumns, fn)

	class BooleanLocalExpression(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> Boolean?): PostgresqlLocalExpression<Boolean>(Boolean::class.java, innerColumns, fn)

	class DateLocalExpression(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> Instant?): PostgresqlLocalExpression<Instant>(Instant::class.java, innerColumns, fn)


	class UuidLocalExpression(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> UUID?): PostgresqlLocalExpression<UUID>(UUID::class.java, innerColumns, fn)

	open class LocalExprDelegate<T: PostgresqlLocalExpression<*>>(private val expr: T) {

		operator fun provideDelegate(thisRef: PostgresTable, prop: KProperty<*>): LocalExprDelegate<T> {
			expr.sqlDataSource = thisRef
			expr.codeName = if (thisRef.joined != null) thisRef.joined!!.codeName + prop.name else prop.name
			thisRef.sqlByNames[prop.name] = expr
			thisRef.namesBySql[expr] = prop.name
			return this
		}

		operator fun getValue(thisRef: PostgresTable, property: KProperty<*>): T {
			return expr
		}
	}

	class StringLocalExpr(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> String?): LocalExprDelegate<StringLocalExpression>(StringLocalExpression(innerColumns, fn))

	class BoolLocalExpr(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> Boolean?): LocalExprDelegate<BooleanLocalExpression>(BooleanLocalExpression(innerColumns, fn))

	class DateLocalExpr(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> Instant?): LocalExprDelegate<DateLocalExpression>(DateLocalExpression(innerColumns, fn))


	class UuidLocalExpr(innerColumns: List<TableColumn<*, *>>, fn: suspend () -> UUID?): LocalExprDelegate<UuidLocalExpression>(UuidLocalExpression(innerColumns, fn))
}