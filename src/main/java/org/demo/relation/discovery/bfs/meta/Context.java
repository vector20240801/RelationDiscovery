package org.demo.relation.discovery.bfs.meta;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;

import java.io.Closeable;
import java.io.Serializable;

/**
 * @author vector
 * @date 2023-08-09 01:12
 */
@Data
@NoArgsConstructor
public class Context implements Closeable, Serializable {
    private static final long serialVersionUID = -5795246437336104048L;
    private SparkSession sparkSession;
    private DataSourceConfiguration dataSourceConfiguration;
    private String env;

    @Override
    public void close() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }
}
