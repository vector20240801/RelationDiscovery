package org.demo.relation.discovery.bfs.loader;

import org.demo.relation.discovery.bfs.meta.Context;
import org.demo.relation.discovery.bfs.util.NamedThreadFactory;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Table;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author vector
 * @date 2023-08-09 01:05
 */
@Service
public class VectorConnectorLoader implements Loader {

    /**
     * @param context user context
     * @return all data loaded
     */
    @Override
    public WholeDataSet load(Context context) throws ExecutionException, InterruptedException, AnalysisException {
        /*
         * query all table from vector_ingestion_table
         */
        context.getSparkSession().sql("USE " + context.getDataSourceConfiguration().getDatasourceNameForVector());
        Dataset<Table> dataSet = context.getSparkSession().catalog().listTables(context.getDataSourceConfiguration().getDatasourceNameForVector());
        /*
         * load data as row
         */
        Iterator<Table> rows = dataSet.toLocalIterator();
        ConcurrentHashMap<String, Dataset<Row>> map = new ConcurrentHashMap<>(1);
        ExecutorService executorService = new ThreadPoolExecutor(context.getDataSourceConfiguration().getLoaderWorkerCount(),
                context.getDataSourceConfiguration().getLoaderWorkerCount(), 1,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(),
                new NamedThreadFactory(context.getDataSourceConfiguration().getDatasourceName()), new ThreadPoolExecutor.AbortPolicy());
        try {
            List<CompletableFuture<Void>> futureList = new ArrayList<>();
            rows.forEachRemaining(item -> {
                String tableName = item.name();
                futureList.add(CompletableFuture.runAsync(() -> {
                    Dataset<Row> rowDataset = context.getSparkSession().table(String.format("%s.%s", context.getDataSourceConfiguration().getDatasourceNameForVector(),
                            tableName));
                    map.put(tableName, rowDataset);
                }, executorService));
            });
            CompletableFuture<Void> future = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
            future.get();
            return new WholeDataSet(map);
        } finally {
            executorService.shutdown();
        }
    }

    @Override
    public LoaderType type() {
        return LoaderType.vector_CONNECTOR;
    }
}
