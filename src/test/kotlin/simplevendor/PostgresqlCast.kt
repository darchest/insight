/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

class PostgresqlCast<SqlT: PostgresqlType>(val from: SqlValue<*, *>, val toType: SqlT): SqlValue<Nothing, SqlT>(Nothing::class.java, toType) {

    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        builder.append("cast(")
        from.writeSql(builder, vendor, params)
        builder.append(" as ", toType.sqlName, ")")
    }

    override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
        from.innerColumns(array)
    }
}