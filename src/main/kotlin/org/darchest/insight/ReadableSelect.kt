/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

interface ReadableSelect<T: SqlDataSource>: Select<T> {

	suspend fun read()
	fun next(): Boolean
	fun close()
}