/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

class PostgresInOperator(val left: SqlValue<*, *>, val values: List<SqlValue<*, *>>, val not: Boolean = false): SqlValue<Boolean, BooleanType>(Boolean::class.java, BooleanType()) {
    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        left.writeSql(builder, vendor, params)
        if (not) builder.append(" NOT")
        builder.append(" IN (")
        val iter = values.iterator()
        var value = iter.next()
        value.writeSql(builder, vendor, params)
        while (iter.hasNext()) {
            builder.append(", ")
            value = iter.next()
            value.writeSql(builder, vendor, params)
        }
        builder.append(")")
    }

    override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
        left.innerColumns(array)
        values.forEach { it.innerColumns(array) }
    }
}