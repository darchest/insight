/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight

import java.sql.Connection
import javax.sql.DataSource

object ConnectionManager {

	data class DataSourceInfo(val name: String, val vendor: Vendor, val dataSource: DataSource)

	private val sources = hashMapOf<String, DataSourceInfo>()

	val localConnection = ThreadLocal<Connection?>()

	fun addDataSource(name: String, vendor: Vendor, source: DataSource) {
		vendor.init()
		sources[name] = DataSourceInfo(name, vendor, source)
	}

	fun getVendor(name: String? = null): Vendor {
		if (sources.isEmpty())
			throw RuntimeException("Data sources aren't registered")
		if (name == null && sources.size == 1)
			return sources.values.first().vendor
		if (name == null)
			throw RuntimeException("Many data sources -> select concrete")
		val ds = sources[name] ?: throw RuntimeException("Data source not found")
		return ds.vendor
	}

	fun getConnection(name: String? = null): Connection {
		var conn = localConnection.get()
		if (conn == null) {
			conn = newConnection(name)
			localConnection.set(conn)
			return conn
		}
		return conn
	}

	fun newConnection(name: String? = null): Connection {
		if (sources.isEmpty())
			throw RuntimeException("Data sources aren't registered")
		if (name == null && sources.size == 1)
			return sources.values.first().dataSource.connection
		if (name == null)
			throw RuntimeException("Many data sources -> select concrete")
		val ds = sources[name] ?: throw RuntimeException("Data source not found")
		return ds.dataSource.connection
	}
}