import kotlinx.coroutines.runBlocking
import org.darchest.insight.impl.select
import simplevendor.PostgresTable
import simplevendor.PostgresVendor
import simplevendor.eq
import simplevendor.gt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class SelectTest {

    class TagTable: PostgresTable("tags") {

        val id by UUIDCol("id")

        val commentId by UUIDCol("comment_id")
    }

    class CommentTable: PostgresTable("comments") {

        val id by UUIDCol("id")

        val userId by UUIDCol("user_id")

        val tags by JoinDelegate(::TagTable, { t -> t.commentId eq id })
    }

    class UserTable : PostgresTable("users") {

        val id by UUIDCol("id")

        val string by VarCharCol("string_col")

        val string10 by VarCharCol("string_col_10", 10)

        val comments by JoinDelegate(::CommentTable, { t -> t.userId eq id })
    }

    class PublicUserTable : PostgresTable("users") {

        override val schemaName: String
            get() = "public"

        val id by UUIDCol("id")

        val string by VarCharCol("string_col")

        val string10 by VarCharCol("string_col_10", 10)

        val comments by JoinDelegate(::DepCommentTable, { t -> t.userId eq id })
    }

    class DepCommentTable(): PostgresTable("comments") {

        var depName: String = ""

        override val schemaName: String
            get() = "dep_$depName"

        val id by UUIDCol("id")

        val userId by UUIDCol("user_id")
    }

    class CycleATable: PostgresTable("a_table") {

        val id by UUIDCol("id")

        val bId by UUIDCol("b_id")
    }

    class CycleBTable: PostgresTable("b_table") {

        val id by UUIDCol("id")

        val aId by UUIDCol("a_id")
    }

    class CycleRootTable: PostgresTable("root") {

        val id by UUIDCol("id")

        val a by JoinDelegate(::CycleATable, { t -> t.id eq id })

        val b by JoinDelegate(::CycleBTable, { t -> t.id eq id })
    }

    class TableWithSection: PostgresTable("with_section") {
        
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
            |SELECT T0."id", T0."string_col", T0."string_col_10", T1."id"
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
            |SELECT T0."id", T0."string_col", T0."string_col_10", T1."id"
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
            |SELECT T0."id", T0."string_col", T0."string_col_10", T1."id"
            |FROM "users" T0
	        |	INNER JOIN "comments" T1 ON T1."user_id" = T0."id"
            |WHERE T0."string_col_10" = ?
            |ORDER BY T0."string_col" DESC
            |LIMIT 10
            |OFFSET 1
        """.trimMargin(), sql)
    }

    @Test
    fun sql_schemas_two_tables_fields() = runBlocking {
        val tbl = PublicUserTable()
        tbl.comments().depName = "main"

        val cursor = select(tbl) {
            fields(tbl.id, tbl.string, tbl.string10, tbl.comments().id)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT T0."id", T0."string_col", T0."string_col_10", T1."id"
            |FROM public."users" T0
	        |	INNER JOIN dep_main."comments" T1 ON T1."user_id" = T0."id"
        """.trimMargin(), sql)
    }

    @Test
    fun sql_group_by() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            groupBy(tbl.id)
            fields(tbl.id, tbl.string)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT "id", "id", "string_col"
            |FROM "users"
            |GROUP BY "id"
        """.trimMargin(), sql)
    }

    @Test
    fun sql_group_by_having() = runBlocking {
        val tbl = UserTable()
        val count = tbl.countExpr<UserTable>()

        val cursor = select(tbl) {
            groupBy(tbl.string)
            fields(count)
            having(count gt 1L)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT "string_col", COUNT(1)
            |FROM "users"
            |GROUP BY "string_col"
            |HAVING COUNT(1) > ?
        """.trimMargin(), sql)
    }

    @Test
    fun sql_group_by_with_join() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            groupBy(tbl.id)
            fields(tbl.comments().id)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT T0."id", T1."id"
            |FROM "users" T0
	        |	INNER JOIN "comments" T1 ON T1."user_id" = T0."id"
            |GROUP BY T0."id"
        """.trimMargin(), sql)
    }

    @Test
    fun sql_nested_joins_order() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            fields(tbl.comments().tags().id)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT T1."id"
            |FROM "users" T0
	        |	INNER JOIN "comments" T2 ON T2."user_id" = T0."id"
	        |	INNER JOIN "tags" T1 ON T1."comment_id" = T2."id"
        """.trimMargin(), sql)
    }

    @Test
    fun sql_nested_joins_child_before_parent_in_fields() = runBlocking {
        val tbl = UserTable()

        val cursor = select(tbl) {
            fields(tbl.comments().tags().id, tbl.comments().id)
        }

        val (sql, _) = cursor.getSql(PostgresVendor)
        assertEquals("""
            |SELECT T1."id", T2."id"
            |FROM "users" T0
	        |	INNER JOIN "comments" T2 ON T2."user_id" = T0."id"
	        |	INNER JOIN "tags" T1 ON T1."comment_id" = T2."id"
        """.trimMargin(), sql)
    }

    @Test
    fun sql_join_cycle_throws() = runBlocking {
        val tbl = CycleRootTable()
        tbl.a()
        tbl.b()
        tbl.a.expr { tbl.a().id eq tbl.b().aId }
        tbl.b.expr { tbl.b().id eq tbl.a().bId }

        val cursor = select(tbl) {
            fields(tbl.a().id, tbl.b().id)
        }

        val ex = runCatching { cursor.getSql(PostgresVendor) }.exceptionOrNull()
        assertIs<IllegalStateException>(ex)
        assertTrue(ex.message!!.contains("Cannot order JOINs"))
    }
}
