import kotlinx.coroutines.runBlocking
import org.darchest.insight.Constraint
import org.darchest.insight.ForeignKey
import org.darchest.insight.PrimaryKey
import org.darchest.insight.Unique
import org.darchest.insight.ddl.CreateTable
import org.junit.jupiter.api.BeforeAll
import simplevendor.PostgresTable
import simplevendor.PostgresVendor
import kotlin.test.Test

class CreateTableTest {

    class UserTable : PostgresTable("users") {

        val id by UUIDCol("id")

        val string by VarCharCol("string_col")

        val string10 by VarCharCol("string_col_10", 10)
    }

    class UserGoodTable : PostgresTable("users") {

        val id by UUIDCol("id")

        val string by VarCharCol("string_col")

        val string10 by VarCharCol("string_col_10", 10)

        override fun constraints(): MutableList<Constraint> {
            return super.constraints().apply {
                add(PrimaryKey(id))
                add(Unique(string))
            }
        }
    }

    class RoleTable : PostgresTable("user_roles") {
        val id by UUIDCol("id")

        val userId by UUIDCol("user_id")

        override fun constraints() = super.constraints().apply {
            add(PrimaryKey(id))
            add(ForeignKey(userId, ::UserTable) { id })
        }
    }

    @Test
    fun sql_1() = runBlocking {
        val tbl = UserTable()

        val crt = CreateTable(tbl)

        val (sql, _) = crt.getSql(PostgresVendor)
        kotlin.test.assertEquals("""
            |CREATE TABLE IF NOT EXISTS "users"
            |(
	        |	id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
	        |	string_col varchar NOT NULL DEFAULT '',
	        |	string_col_10 varchar(10) NOT NULL DEFAULT ''
            |);
        """.trimMargin(), sql)
    }

    @Test
    fun sql_2() = runBlocking {
        val tbl = UserGoodTable()

        val crt = CreateTable(tbl)

        val (sql, _) = crt.getSql(PostgresVendor)
        kotlin.test.assertEquals("""
            |CREATE TABLE IF NOT EXISTS "users"
            |(
	        |	id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
	        |	string_col varchar NOT NULL DEFAULT '',
	        |	string_col_10 varchar(10) NOT NULL DEFAULT '',
            |	PRIMARY KEY (id),
            |	UNIQUE (string_col)
            |);
        """.trimMargin(), sql)
    }

    @Test
    fun sql_3() = runBlocking {
        val tbl = RoleTable()

        val crt = CreateTable(tbl)

        val (sql, _) = crt.getSql(PostgresVendor)
        kotlin.test.assertEquals("""
            |CREATE TABLE IF NOT EXISTS "user_roles"
            |(
	        |	id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
	        |	user_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
            |	PRIMARY KEY (id),
            |	FOREIGN KEY (user_id) REFERENCES users (id)
            |);
        """.trimMargin(), sql)
    }

    companion object {
        @JvmStatic
        @BeforeAll
        fun beforeAll(): Unit {
            PostgresVendor.init()
        }
    }
}