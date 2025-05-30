/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

abstract class MaterialView(sqlName: String): View(sqlName) {
	abstract fun refresh()
}