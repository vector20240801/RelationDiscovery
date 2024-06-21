package com.test.demo;

import org.demo.relation.discovery.bfs.ContextService;
import org.demo.relation.discovery.bfs.Global;
import org.demo.relation.discovery.bfs.meta.Context;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

/**
 * @author vector
 * @date 2023-11-01 21:27
 */
public class TableCreate {
    public static void main(String[] args) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("relation_id", DataTypes.StringType, true),
                DataTypes.createStructField("table_name_a", DataTypes.StringType, true),
                DataTypes.createStructField("column_set_a", DataTypes.StringType, true),
                DataTypes.createStructField("column_set_a_size", DataTypes.IntegerType, true),
                DataTypes.createStructField("table_name_b", DataTypes.StringType, true),
                DataTypes.createStructField("column_set_b", DataTypes.StringType, true),
                DataTypes.createStructField("column_set_b_size", DataTypes.IntegerType, true),
                DataTypes.createStructField("create_time", DataTypes.DateType, true),
                DataTypes.createStructField("status", DataTypes.IntegerType, true),
                DataTypes.createStructField("datasource_name", DataTypes.StringType, true),
        });
        StructType LINE_SCHEMA = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("line_id", DataTypes.StringType, true),
                DataTypes.createStructField("table_name", DataTypes.StringType, true),
                DataTypes.createStructField("column_set", DataTypes.StringType, true),
                DataTypes.createStructField("column_set_size", DataTypes.IntegerType, true),
                DataTypes.createStructField("column_set_value", DataTypes.StringType, true),
                DataTypes.createStructField("create_time", DataTypes.DateType, true),
                DataTypes.createStructField("status", DataTypes.IntegerType, true),
                DataTypes.createStructField("datasource_name", DataTypes.StringType, true),
        });
        try (Context context = Global.getAppCtx().getBean(ContextService.class).load("test-local", "vector_data_service")) {
            // Create the database
            context.getSparkSession().createDataFrame(Collections.emptyList(), LINE_SCHEMA).write().mode(SaveMode.Ignore).saveAsTable("vector_data_service.columnset_line");
            context.getSparkSession().createDataFrame(Collections.emptyList(), schema).write().mode(SaveMode.Ignore).saveAsTable("vector_data_service.columnset_relation");
        }
    }
}
