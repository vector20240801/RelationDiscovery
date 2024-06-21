package org.demo.relation.discovery.bfs.loader;

import org.demo.relation.discovery.bfs.meta.Context;
import org.apache.spark.sql.AnalysisException;

import java.util.concurrent.ExecutionException;

/**
 * @author vector
 * @date 2023-08-09 01:05
 */
public interface Loader {
    /**
     * load from extra datasource
     *
     * @param context user context
     * @return
     */
    WholeDataSet load(Context context) throws ExecutionException, InterruptedException, AnalysisException;

    /**
     * loader type
     */
    LoaderType type();
}
