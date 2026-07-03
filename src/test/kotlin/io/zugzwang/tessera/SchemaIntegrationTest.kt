package io.zugzwang.tessera

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.BeforeEach
import org.postgresql.util.PSQLException
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@Testcontainers
class SchemaIntegrationTest {

    companion object {
        // A database of our own: tests here run sequentially (JUnit default),
        // but a dedicated database means enabling parallel execution across
        // classes later can never silently share this state.
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:18.4").withDatabaseName("schema_it")
    }

    private val dataSource: DataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = postgres.jdbcUrl
            username = postgres.username
            password = postgres.password
            maximumPoolSize = 2
        },
    )

    private fun execute(sql: String) = dataSource.connection.use { connection ->
        connection.createStatement().use { it.execute(sql) }
    }

    @BeforeEach
    fun resetSchema() {
        execute("DROP TABLE IF EXISTS latest_state, change_history, projector_checkpoint")
    }

    @Test
    fun `ensure creates the three tables and is idempotent`() {
        Schema.ensure(dataSource)
        Schema.ensure(dataSource)

        val tables = dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement
                    .executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
                    .use { rs -> generateSequence { if (rs.next()) rs.getString(1) else null }.toList() }
            }
        }
        assertEquals(listOf("change_history", "latest_state", "projector_checkpoint"), tables)
    }

    @Test
    fun `history rejects a tombstone carrying a value and a put without one`() {
        Schema.ensure(dataSource)

        // The CHECK constraint is the schema-level mirror of the sealed
        // Put/Tombstone model: NULL value ⟺ tombstone.
        assertFailsWith<PSQLException> {
            execute("INSERT INTO change_history VALUES (0, 0, 'orders', 'a', '\\x01', true)")
        }
        assertFailsWith<PSQLException> {
            execute("INSERT INTO change_history VALUES (0, 0, 'orders', 'a', NULL, false)")
        }
        execute("INSERT INTO change_history VALUES (0, 0, 'orders', 'a', NULL, true)")
        execute("INSERT INTO change_history VALUES (0, 1, 'orders', 'b', '\\x', false)")
    }

    @Test
    fun `history identity is the log coordinate`() {
        Schema.ensure(dataSource)

        execute("INSERT INTO change_history VALUES (10, 0, 'orders', 'a', '\\x01', false)")
        // Same coordinates = same entry, regardless of content.
        assertFailsWith<PSQLException> {
            execute("INSERT INTO change_history VALUES (10, 0, 'orders', 'z', '\\x02', false)")
        }
        // Same key again at a different position is legal history.
        execute("INSERT INTO change_history VALUES (10, 1, 'orders', 'a', '\\x02', false)")
    }
}
