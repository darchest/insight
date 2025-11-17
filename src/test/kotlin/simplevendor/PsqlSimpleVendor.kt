package simplevendor

import org.darchest.insight.*
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Instant
import java.util.*

object PostgresVendor: Vendor {

    private abstract class DefaultNullTypeConverter: SqlTypeConverter {
        override fun javaToSql(value: Any?): String {
            if (value == null)
                return "NULL"
            return notNullJavaToSql(value)
        }

        override fun javaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any?) {
            if (value == null) {
                ps.setNull(ind, 1)
                return
            }
            notNullJavaToPreparedSql(ps, ind, value)
        }

        abstract fun notNullJavaToSql(value: Any): String

        abstract fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any)
    }

    override fun init() {
        initUuidType()
        initCharTypes()
        initNumberTypes()
        initByteaTypes()
        initUuidArrayType()
        initBooleanType()
    }

    private fun initUuidType() {
        SqlTypeConvertersRegistry.registerConverter(UUID::class.java, UuidType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "'$value'"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setObject(ind, value, Types.OTHER)

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getObject(ind, UUID::class.java)
        })
    }

    private fun initCharTypes() {
        val strConv = object : DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "'${value.toString().replace("'", "''")}'"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setString(ind, value as String)

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getString(ind)
        }

        SqlTypeConvertersRegistry.registerConverter(String::class.java, VarCharType::class.java, strConv)
        SqlTypeConvertersRegistry.registerConverter(String::class.java, CharType::class.java, strConv)
    }

    private fun initNumberTypes() {
        SqlTypeConvertersRegistry.registerConverter(Int::class.java, IntType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "$value"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setInt(ind, value as Int)

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getInt(ind)
        })
        SqlTypeConvertersRegistry.registerConverter(Integer::class.java, IntType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "$value"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setInt(ind, value as Int)

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getInt(ind)
        })
        SqlTypeConvertersRegistry.registerConverter(Long::class.java, BigIntType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "$value"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setLong(ind, value as Long)

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getLong(ind)
        })
        SqlTypeConvertersRegistry.registerConverter(Instant::class.java, BigIntType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "${(value as Instant).toEpochMilli()}"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setLong(ind, (value as Instant).toEpochMilli())

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = Instant.ofEpochMilli(rs.getLong(ind))
        })
    }

    private fun initByteaTypes() {
        SqlTypeConvertersRegistry.registerConverter(ByteArray::class.java, ByteaType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "$value"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setBytes(ind, value as ByteArray)

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getBytes(ind)
        })
        SqlTypeConvertersRegistry.registerConverter(String::class.java, ByteaType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "'$value'"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setBytes(ind, (value as String).toByteArray())

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getBytes(ind).toString(Charsets.UTF_8)
        })
    }

    private fun initUuidArrayType() {
        SqlTypeConvertersRegistry.registerConverter(UUIDArray::class.java, UuidArrayType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String {
                val arr = value as UUIDArray
                return "'{" + arr.joinToString(",") { "$it" } + "}'"
            }

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) {
                val sqlArr = (value as UUIDArray).toArray()
                ps.setArray(ind, ps.connection.createArrayOf("uuid", sqlArr))
            }

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = UUIDArray().apply { addAll(rs.getArray(ind) as Array<UUID>) }
        })
    }

    private fun initBooleanType() {
        SqlTypeConvertersRegistry.registerConverter(Boolean::class.java, BooleanType::class.java, object: DefaultNullTypeConverter() {
            override fun notNullJavaToSql(value: Any): String = "$value"

            override fun notNullJavaToPreparedSql(ps: PreparedStatement, ind: Int, value: Any) = ps.setBoolean(ind, value as Boolean)

            override fun sqlToJava(rs: ResultSet, ind: Int): Any? = rs.getBoolean(ind)
        })
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