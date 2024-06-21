package org.demo.relation.discovery.bfs.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;


/**
 * @author vector
 * @date 2023-08-11 21:35
 */
@Configuration
@PropertySource({"classpath:bfs.properties"})
public class BFSConfiguration {
    @Value("${address.hive:jdbc:hive2://localhost:10000/default;auth=noSasl}")
    private String hiveAddress;
    @Value("${user.hive:hive}")
    private String hiveUserName;
    @Value("${password.hive:hive}")
    private String hivePassword;

    @Bean
    public JdbcTemplate jdbcTemplate() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(hiveAddress);
        hikariConfig.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        hikariConfig.setPassword(hivePassword);
        hikariConfig.setUsername(hiveUserName);
        hikariConfig.setMinimumIdle(4);
        hikariConfig.setMaximumPoolSize(128);
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        return new JdbcTemplate(dataSource);
    }

}
