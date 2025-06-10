/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.SqlType

open class PostgresqlType(name: String): SqlType(name)

open class BooleanType: PostgresqlType("bool")

open class ByteaType: PostgresqlType("bytea")

open class NumericType(name: String): PostgresqlType(name)

open class SmallIntType: NumericType("smallint")

open class IntType: NumericType("integer")

open class BigIntType: NumericType("bigint")

open class UuidType: PostgresqlType("uuid")

open class UuidArrayType: PostgresqlType("uuid[]")

open class StringType(name: String): PostgresqlType(name)

open class CharType: StringType("char")

open class VarCharType: StringType("varchar")
