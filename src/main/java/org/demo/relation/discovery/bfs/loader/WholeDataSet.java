package org.demo.relation.discovery.bfs.loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

/**
 * @author vector
 * @date 2023-08-11 15:55
 */
public class WholeDataSet {
    private final Map<String, Dataset<Row>> datasetMap;


    public WholeDataSet(Map<String, Dataset<Row>> datasetMap) {
        this.datasetMap = datasetMap;
    }

    public Dataset<Row> queryAllRowsByTable(String tableName) {
        return datasetMap.get(tableName);
    }

    public Map<String, Dataset<Row>> getDatasetMap() {
        return datasetMap;
    }
}
