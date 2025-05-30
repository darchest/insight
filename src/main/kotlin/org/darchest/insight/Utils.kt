/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import java.util.*

val JsonElement.asUUID: UUID
    get() = UUID.fromString(asString)

fun JsonObject.getAsUUID(memberName: String): UUID = get(memberName).asUUID