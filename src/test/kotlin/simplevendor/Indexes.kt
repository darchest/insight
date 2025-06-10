/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package simplevendor

import org.darchest.insight.Index
import org.darchest.insight.SqlValue
import org.darchest.insight.Vendor

class PostresqlIndex(val col: PostgresqlColumn<*, *>, override val unique: Boolean = false): Index(col) {

    override val name = col.name + "_idx"

    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        TODO("Not yet implemented")
    }
}

fun PostgresTable.index(col: PostgresqlColumn<*, *>) = PostresqlIndex(col, false)