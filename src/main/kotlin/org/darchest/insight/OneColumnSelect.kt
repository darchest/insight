/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

/**
 * SELECT with exactly one field in the select list.
 * Basis for scalar subquery expressions ([org.darchest.insight.expression.SelectExpression]).
 */
interface OneColumnSelect<T : SqlDataSource> : Select<T> {
	/** The single field of the select list. */
	fun column(): SqlValue<*, *>
}
