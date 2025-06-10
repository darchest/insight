/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

class PostgresLike(val left: SqlValue<*, *>, val right: SqlValue<*, *>): SqlValue<Boolean, BooleanType>(Boolean::class.java, BooleanType()) {

    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        left.writeSql(builder, vendor, params)
        builder.append(" LIKE ")
        right.writeSql(builder, vendor, params)
    }
}

class PostgresILike(val left: SqlValue<*, *>, val right: SqlValue<*, *>): SqlValue<Boolean, BooleanType>(Boolean::class.java, BooleanType()) {

    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        left.writeSql(builder, vendor, params)
        builder.append(" ILIKE ")
        right.writeSql(builder, vendor, params)
    }

    override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
        left.innerColumns(array)
        right.innerColumns(array)
    }
}