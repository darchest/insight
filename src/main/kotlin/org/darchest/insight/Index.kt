/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class Index(vararg val columns: SqlValue<*, *>): SqlPrintable {

    abstract val unique: Boolean

    abstract val name: String
}

fun MutableList<Index>.addIndexes(vararg indexes: Index): MutableList<Index> {
    indexes.forEach {
        this.add(it)
    }
    return this
}