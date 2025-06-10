package simplevendor

import org.darchest.insight.*
import java.sql.Types
import java.time.Instant
import java.util.*

object PostgresVendor: Vendor {

    override fun init() {
        sqlToJava()
        javaToSql()
        javaToPrepSql()
    }

    private fun sqlToJava() {
        SqlTypeConverter.registerSqlToJava(UuidType::class.java, UUID::class.java) { rs, ind ->
            rs.getObject(ind, UUID::class.java)
        }
        SqlTypeConverter.registerSqlToJava(VarCharType::class.java, String::class.java) { rs, ind ->
            rs.getString(ind)
        }
        SqlTypeConverter.registerSqlToJava(CharType::class.java, String::class.java) { rs, ind ->
            rs.getString(ind)
        }
        SqlTypeConverter.registerSqlToJava(BooleanType::class.java, Boolean::class.java) { rs, ind ->
            rs.getBoolean(ind)
        }
        SqlTypeConverter.registerSqlToJava(IntType::class.java, Int::class.java) { rs, ind ->
            rs.getInt(ind)
        }
        SqlTypeConverter.registerSqlToJava(BigIntType::class.java, Long::class.java) { rs, ind ->
            rs.getLong(ind)
        }
        SqlTypeConverter.registerSqlToJava(BigIntType::class.java, Instant::class.java) { rs, ind ->
            Instant.ofEpochMilli(rs.getLong(ind))
        }
        SqlTypeConverter.registerSqlToJava(ByteaType::class.java, String::class.java) { rs, ind ->
            rs.getBytes(ind).toString(Charsets.UTF_8)
        }
        SqlTypeConverter.registerSqlToJava(ByteaType::class.java, ByteArray::class.java) { rs, ind ->
            rs.getBytes(ind)
        }

        SqlTypeConverter.registerSqlToJava(UuidArrayType::class.java, UUIDArray::class.java) { rs, ind ->
            UUIDArray().apply { addAll(rs.getArray(ind) as Array<UUID>) }
        }
    }

    private fun javaToSql() {
        SqlTypeConverter.registerJavaToSql(UUID::class.java, UuidType::class.java) { uuid ->
            if (uuid == null) "NULL" else "'$uuid'"
        }
        SqlTypeConverter.registerJavaToSql(String::class.java, UuidType::class.java) { uuid ->
            if (uuid == null) "NULL" else "'${UUID.fromString(uuid.toString())}'"
        }

        SqlTypeConverter.registerJavaToSql(String::class.java, VarCharType::class.java) { str ->
            if (str == null) "NULL" else "'${str.toString().replace("'", "''")}'"
        }
        SqlTypeConverter.registerJavaToSql(String::class.java, CharType::class.java) { str ->
            if (str == null) "NULL" else "'${str.toString().replace("'", "''")}'"
        }

        SqlTypeConverter.registerJavaToSql(Boolean::class.java, BooleanType::class.java) { str ->
            if (str == null) "NULL" else "$str"
        }
        SqlTypeConverter.registerJavaToSql(String::class.java, BooleanType::class.java) { boolStr ->
            if (boolStr == null) "NULL" else if (java.lang.Boolean.parseBoolean(boolStr.toString())) "true" else "false"
        }
        SqlTypeConverter.registerJavaToSql(Instant::class.java, BigIntType::class.java) { dt ->
            if (dt == null)"NULL" else "${(dt as Instant).toEpochMilli()}"
        }
        SqlTypeConverter.registerJavaToSql(String::class.java, BigIntType::class.java) { dt ->
            if (dt == null)"NULL" else (dt as String).toLong().toString()
        }
        SqlTypeConverter.registerJavaToSql(Int::class.java, IntType::class.java) { num ->
            if (num == null)"NULL" else "$num"
        }
        SqlTypeConverter.registerJavaToSql(Integer::class.java, IntType::class.java) { num ->
            if (num == null)"NULL" else "$num"
        }
        SqlTypeConverter.registerJavaToSql(Long::class.java, BigIntType::class.java) { num ->
            if (num == null)"NULL" else "$num"
        }
        SqlTypeConverter.registerJavaToSql(ByteArray::class.java, ByteaType::class.java) { num ->
            if (num == null)"NULL" else "$num"
        }

        SqlTypeConverter.registerJavaToSql(String::class.java, ByteaType::class.java) { num ->
            if (num == null)"NULL" else "'$num'"
        }

        SqlTypeConverter.registerJavaToSql(UUIDArray::class.java, UuidArrayType::class.java) { uuid ->
            if (uuid == null) "NULL"
            else {
                val arr = uuid as UUIDArray
                "'{" + arr.joinToString(",") { "$it" } + "}'"
            }
        }
    }

    private fun javaToPrepSql() {
        SqlTypeConverter.registerJavaToPrepSql(UUID::class.java, UuidType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setObject(ind, any, Types.OTHER)
        }
        SqlTypeConverter.registerJavaToPrepSql(String::class.java, UuidType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setObject(ind, UUID.fromString(any as String), Types.OTHER)
        }

        SqlTypeConverter.registerJavaToPrepSql(String::class.java, VarCharType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setString(ind, any as String)
        }
        SqlTypeConverter.registerJavaToPrepSql(String::class.java, CharType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setString(ind, any as String)
        }

        SqlTypeConverter.registerJavaToPrepSql(Boolean::class.java, BooleanType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setBoolean(ind, any as Boolean)
        }
        SqlTypeConverter.registerJavaToPrepSql(java.lang.Boolean::class.java, BooleanType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setBoolean(ind, any as Boolean)
        }
        SqlTypeConverter.registerJavaToPrepSql(String::class.java, BooleanType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setBoolean(ind, java.lang.Boolean.parseBoolean(any.toString()))
        }
        SqlTypeConverter.registerJavaToPrepSql(Instant::class.java, BigIntType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setLong(ind, (any as Instant).toEpochMilli())
        }
        SqlTypeConverter.registerJavaToPrepSql(Int::class.java, IntType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setInt(ind, any as Int)
        }
        SqlTypeConverter.registerJavaToPrepSql(Integer::class.java, IntType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setInt(ind, any as Int)
        }
        SqlTypeConverter.registerJavaToPrepSql(Long::class.java, BigIntType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setLong(ind, any as Long)
        }
        SqlTypeConverter.registerJavaToPrepSql(ByteArray::class.java, ByteaType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1) else ps.setBytes(ind, any as ByteArray)
        }

        SqlTypeConverter.registerJavaToPrepSql(UUIDArray::class.java, UuidArrayType::class.java) { ps, ind, any ->
            if (any == null) ps.setNull(ind, 1)
            else {
                val sqlArr = (any as UUIDArray).toArray()
                ps.setArray(ind, ps.connection.createArrayOf("uuid", sqlArr))
            }
        }
    }

    override fun isBoolean(type: Class<out SqlType>) = BooleanType::class.java.isAssignableFrom(type)

    override fun getTables(dataSourceName: String) {
        val vendor = ConnectionManager.getVendor(dataSourceName)
        if (vendor != this)
            throw RuntimeException("This is not me")
        val connection = ConnectionManager.getConnection(dataSourceName)
        val meta = connection.metaData
        var rs = meta.getTables(null, null, "%", arrayOf("TABLE"))
        val tables = mutableListOf<SqlDataSourceInfo>()
        while (rs.next()) {
            tables.add(SqlDataSourceInfo(rs.getString(3)))
        }
        rs = meta.getColumns(null, null, null, null)
        while (rs.next()) {
            val tbl = tables.firstOrNull { it.name.equals(rs.getString(3)) }
            tbl?.columns?.add(ColumnInfo(rs.getString(4), rs.getString(6)))
        }


        tables.forEach {
            println(it.name)
            it.columns.forEach { c ->
                println("\t${c.name} ${c.type}")
            }
        }
    }

    override fun writeSqlDataSource(dataSource: SqlDataSource, builder: StringBuilder) {
        builder.append("\"")
        builder.append(dataSource.sqlName)
        builder.append("\"")
    }

    override fun writeSqlColumnName(column: TableColumn<*, *>, builder: StringBuilder) {
        builder.append("\"");
        builder.append(column.name);
        builder.append("\"");
    }

    override fun createLogicalOperation(op: LogicalOperation.Operator, values: Collection<SqlValue<*, *>>): LogicalOperation<*, *> {
        return PostgresqlLogical(op, values)
    }

    override fun createComparisonOperation(left: SqlValue<*, *>, op: ComparisonOperation.Operator, right: SqlValue<*, *>): ComparisonOperation<*, *> {
        return PostgresqlComparison(left, op, right)
    }

    override fun getCountExpression(): Expression<Long, *> {
        return CountExpression()
    }
}