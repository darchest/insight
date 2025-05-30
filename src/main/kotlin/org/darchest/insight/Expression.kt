/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class Expression<javaType: Any, sqlT: SqlType>(javaClass: Class<javaType>, sqlType: sqlT): SqlValue<javaType, sqlT>(javaClass, sqlType), SqlValueNotNullGetter<javaType> {

    override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
        super.fillByInnerColumns(array)
        array.add(this)
    }
}