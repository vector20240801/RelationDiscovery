package com.test.demo;

import org.demo.relation.discovery.bfs.ContextService;
import org.demo.relation.discovery.bfs.DatasourceService;
import org.demo.relation.discovery.bfs.Global;
import org.demo.relation.discovery.bfs.meta.Context;

import java.io.IOException;

/**
 * @author vector
 * @date 2023-07-08 9:48 PM
 */
public class TestSparkAndTrino {
    /**
     * create table vector_data_service.test1
     * (
     * id              integer,
     * citem_uuid    varchar(128),
     * corder_id      varchar(128)
     * );
     * create table vector_data_service.test2
     * (
     * id              integer,
     * primary_id      varchar(128),
     * corder2_id      varchar(128),
     * citem_uuid    varchar(128)
     * );
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        /*
         * datasourceName test-local
         * datasourceNameForvector vector_data_service
         *
         */
        try (Context context = Global.getAppCtx().getBean(ContextService.class).load("test-local", "vector_data_service")) {
            Global.getAppCtx().getBean(DatasourceService.class).reloadTableRelation(context);
            System.out.println(context.getSparkSession().sql("select * from vector_data_service.columnset_line where status=1").count());
            context.getSparkSession().sql("select * from vector_data_service.columnset_relation where status=1").show(1000);
        }
    }
}
