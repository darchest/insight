import kotlinx.coroutines.runBlocking
import org.darchest.insight.impl.select
import simplevendor.PostgresTable
import simplevendor.PostgresVendor
import simplevendor.eq
import kotlin.test.Test
import kotlin.test.assertEquals

class SelectTest {

    class CommentTable: PostgresTable("comments") {

        val id by UUIDCol("id")

        val userId by UUIDCol("user_id")
    }

    class UserTable : PostgresTable("users") {

        val id by UUIDCol("id")

        val string by VarCharCol("string_col")

        val string10 by VarCharCol("string_col_10", 10)

        val comments by JoinDelegate(::CommentTable, { t -> t.userId eq id })
    }

    @Test
    fun sql_one_table_fields() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            fields(tbl.id, tbl.string, tbl.string10)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT "id", "string_col", "string_col_10"
            |FROM "users"
        """.trimMargin(), sql)
    }

    @Test
    fun sql_two_tables_fields() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            fields(tbl.id, tbl.string, tbl.string10, tbl.comments().id)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT T0."id", T0."string_col", T0."string_col_10", T1."user_id", T1."id"
            |FROM "users" T0
	        |	INNER JOIN "comments" T1 ON T1."user_id" = T0."id"
        """.trimMargin(), sql)
    }

    @Test
    fun sql_two_tables_fields_where_order() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            fields(tbl.id, tbl.string, tbl.string10, tbl.comments().id)
            where(tbl.string10 eq "hello")
            sort(tbl.string.desc())
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT T0."id", T0."string_col", T0."string_col_10", T1."user_id", T1."id"
            |FROM "users" T0
	        |	INNER JOIN "comments" T1 ON T1."user_id" = T0."id"
            |WHERE T0."string_col_10" = ?
            |ORDER BY T0."string_col" DESC
        """.trimMargin(), sql)
    }

    @Test
    fun sql_two_tables_fields_where_order_limit_offset() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            fields(tbl.id, tbl.string, tbl.string10, tbl.comments().id)
            where(tbl.string10 eq "hello")
            sort(tbl.string.desc())
            limit(10)
            offset(1)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT T0."id", T0."string_col", T0."string_col_10", T1."user_id", T1."id"
            |FROM "users" T0
	        |	INNER JOIN "comments" T1 ON T1."user_id" = T0."id"
            |WHERE T0."string_col_10" = ?
            |ORDER BY T0."string_col" DESC
            |LIMIT 10
            |OFFSET 1
        """.trimMargin(), sql)
    }
}