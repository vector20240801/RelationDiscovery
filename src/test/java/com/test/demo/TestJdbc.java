package com.test.demo;

import com.alibaba.fastjson.JSON;
import org.demo.relation.discovery.bfs.Global;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * @author vector
 * @date 2023-11-03 00:21
 */
public class TestJdbc {
    public static void main(String[] args) {
        JdbcTemplate jdbcTemplate = Global.getAppCtx().getBean(JdbcTemplate.class);
        List<Map<String, Object>> list = jdbcTemplate.queryForList("select * from vector_fieldbfs.columnset_relation limit 100");
        System.out.println(JSON.toJSONString(list));
    }
}
