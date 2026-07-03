package com.alerting.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
@Configuration
public class ClickHouseConfig {

    // Explicitly define the primary (PostgreSQL) datasource so that
    // Spring Boot's DataSourceAutoConfiguration does not back off when
    // it sees the ClickHouse DataSource bean below.
    @Primary
    @Bean(name = "dataSource")
    public DataSource dataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password,
            @Value("${spring.datasource.driver-class-name}") String driverClassName) {
        return DataSourceBuilder.create()
                .url(url)
                .username(username)
                .password(password)
                .driverClassName(driverClassName)
                .build();
    }

    @Value("${clickhouse.url}")
    private String chUrl;

    @Value("${clickhouse.username}")
    private String chUsername;

    @Value("${clickhouse.password}")
    private String chPassword;

    @Bean(name = "clickHouseDataSource")
    public DataSource clickHouseDataSource() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", chUsername);
        props.setProperty("password", chPassword);
        return new ClickHouseDataSource(chUrl, props);
    }

    @Bean(name = "clickHouseJdbcTemplate")
    public JdbcTemplate clickHouseJdbcTemplate() throws SQLException {
        return new JdbcTemplate(clickHouseDataSource());
    }
}
