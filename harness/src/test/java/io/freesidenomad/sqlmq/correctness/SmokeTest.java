package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.support.SqlServerContainer;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class SmokeTest {

    @Test
    void containerStartsAndAcceptsJdbcConnection() throws Exception {
        var container = SqlServerContainer.get();
        try (Connection conn = java.sql.DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT @@VERSION AS v")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("v")).contains("Microsoft SQL Server 2022");
        }
    }
}
