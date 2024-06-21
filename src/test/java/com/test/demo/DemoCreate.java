package com.test.demo;

import org.demo.relation.discovery.bfs.ContextService;
import org.demo.relation.discovery.bfs.Global;
import org.demo.relation.discovery.bfs.meta.Context;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author vector
 * @date 2023-11-01 21:11
 */
public class DemoCreate {
    public static void main(String[] args) {
        try (Context context = Global.getAppCtx().getBean(ContextService.class).load("test-local", "vector_data_service")) {
            // Create the database
            context.getSparkSession().sql("CREATE DATABASE IF NOT EXISTS vector_data_service");
            context.getSparkSession().sql("USE vector_data_service");

            // Create the schema for the tables
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("col1", DataTypes.IntegerType, false),
                    DataTypes.createStructField("col2", DataTypes.IntegerType, false)
            });

            // Create test1 table with random data
            Dataset<Row> test1Data = context.getSparkSession().range(1000).selectExpr("CAST(rand() * 1000 AS INT) AS col1", "CAST(rand() * 1000 AS INT) AS col2");
            test1Data.write().mode(SaveMode.Overwrite).saveAsTable("test1");

            // Create test2 table with random data
            Dataset<Row> test2Data = context.getSparkSession().range(1000).selectExpr("CAST(rand() * 1000 AS INT) AS col1", "CAST(rand() * 1000 AS INT) AS col2");
            test2Data.write().mode(SaveMode.Overwrite).saveAsTable("test2");
        }
    }
}
