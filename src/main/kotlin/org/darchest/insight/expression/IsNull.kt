/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.expression

import org.darchest.insight.SqlDataSource
import org.darchest.insight.SqlType
import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

open class IsNull<sqlT: SqlType>(
    private val value: SqlValue<*, *>,
    sqlType: sqlT,
): SqlValue<Boolean, sqlT>(Boolean::class.java, sqlType) {
    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        value.writeSql(builder, vendor, params)
        builder.append(" IS NULL")
    }

    override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
        value.innerColumns(array)
    }

    override fun collectReferencedSources(out: MutableSet<SqlDataSource>) {
        value.collectReferencedSources(out)
    }
}
