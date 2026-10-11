package org.darchest.insight

class SqlCase<javaType : Any, sqlT : SqlType>(
    val cases: List<Pair<SqlValue<*, *>, SqlValue<javaType, sqlT>>>,
    val elseValue: SqlValue<javaType, sqlT>,
    javaClass: Class<javaType>,
    sqlType: sqlT,
) : SqlValue<javaType, sqlT>(javaClass, sqlType) {

    init {
        require(cases.isNotEmpty()) { "CASE must contain at least one WHEN clause" }
    }

    override suspend fun writeSql(builder: StringBuilder, vendor: Vendor, params: MutableList<SqlValue<*, *>>) {
        builder.append("CASE")
        for ((condition, value) in cases) {
            builder.append(" WHEN ")
            condition.writeSql(builder, vendor, params)
            builder.append(" THEN ")
            value.writeSql(builder, vendor, params)
        }
        builder.append(" ELSE ")
        elseValue.writeSql(builder, vendor, params)
        builder.append(" END")
    }

    override fun fillByInnerColumns(array: MutableCollection<SqlValue<*, *>>) {
        cases.forEach { (condition, value) ->
            condition.innerColumns(array)
            value.innerColumns(array)
        }
        elseValue.innerColumns(array)
    }

    override fun collectReferencedSources(out: MutableSet<SqlDataSource>) {
        cases.forEach { (condition, value) ->
            condition.collectReferencedSources(out)
            value.collectReferencedSources(out)
        }
        elseValue.collectReferencedSources(out)
    }
}
