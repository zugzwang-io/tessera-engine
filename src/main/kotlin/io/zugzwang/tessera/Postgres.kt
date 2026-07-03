package io.zugzwang.tessera

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import javax.sql.DataSource

object Postgres {
    fun dataSource(config: Config): DataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = config.postgresUrl
            username = config.postgresUser
            password = config.postgresPassword
            // Hikari's default pool size (10); revisit with a measured reason
            // once the read path carries real load.
        },
    )
}
