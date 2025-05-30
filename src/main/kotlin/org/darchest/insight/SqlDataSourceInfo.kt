/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

class SqlDataSourceInfo(val name: String) {

	val columns = mutableListOf<ColumnInfo>()
}

class ColumnInfo(val name: String, val type: String) {

}