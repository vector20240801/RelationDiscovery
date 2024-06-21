package com.test.demo;

import org.demo.relation.discovery.bfs.ContextService;
import org.demo.relation.discovery.bfs.Global;
import org.demo.relation.discovery.bfs.PostCheckService;
import org.demo.relation.discovery.bfs.meta.Context;

/**
 * @author vector
 * @date 2023-11-03 21:32
 */
public class TestPostCheck {
    public static void main(String[] args) {
        try (Context context = Global.getAppCtx().getBean(ContextService.class).load("test-local", "vector_data_service")) {
            context.getDataSourceConfiguration().setDatasourceNameForVector("64d5c22b7afcda3522b39e71");
            context.getDataSourceConfiguration().setDatasourceName("64d5c22b7afcda3522b39e71");
            Global.getAppCtx().getBean(PostCheckService.class).postCheckRelation(context);
        }
    }
}
