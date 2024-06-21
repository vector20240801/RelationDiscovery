package org.demo.relation.discovery.bfs;

import org.demo.relation.discovery.bfs.meta.Context;

import java.io.Serializable;

/**
 * @author vector
 * @date 2023-08-09 00:07
 */
public class FieldBFSCommander implements Serializable {


    private static final long serialVersionUID = -4262765050939274237L;

    public static void reloadTableRelation(String datasourceName, String vectorDatasourceName) {
        long start = System.currentTimeMillis();
        try (Context context = Global.getAppCtx().getBean(ContextService.class).load(datasourceName, vectorDatasourceName)) {
            Global.getAppCtx().getBean(DatasourceService.class).reloadTableRelation(context);
            Global.getAppCtx().getBean(PostCheckService.class).postCheckRelation(context);
        }
        System.out.printf("[FieldBFSCommander] cost %d ms\n", (System.currentTimeMillis() - start));
    }

    public static void createTable() {
        long start = System.currentTimeMillis();
        try (Context context = Global.getAppCtx().getBean(ContextService.class).load("init", "init")) {
            Global.getAppCtx().getBean(DatasourceService.class).createTable(context);
        }
        System.out.printf("[FieldBFSCommander] cost %d ms\n", (System.currentTimeMillis() - start));

    }

}
