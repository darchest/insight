/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.expression

import org.darchest.insight.Expression
import org.darchest.insight.SqlDataSource
import org.darchest.insight.SqlType
import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

class Max<javaType : Any, sqlType : SqlType>(
    val field: SqlValue<javaType, sqlType>
): Expression<javaType, sqlType>(field.javaClass, field.sqlType) {
    override suspend fun writeSql(
        builder: StringBuilder,
        vendor: Vendor,
        params: MutableList<SqlValue<*, *>>
    ) {
        builder.append("max(")
        field.writeSql(builder, vendor, params)
        builder.append(")")
    }

    override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
        super.fillByInnerColumns(array)
        field.innerColumns(array)
    }

    override fun collectReferencedSources(out: MutableSet<SqlDataSource>) {
        field.collectReferencedSources(out)
    }
}

fun <javaType : Any, sqlType : SqlType> max(field: SqlValue<javaType, sqlType>) = Max(field)