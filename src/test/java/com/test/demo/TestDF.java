package com.test.demo;

import org.demo.relation.discovery.bfs.ContextService;
import org.demo.relation.discovery.bfs.Global;
import org.demo.relation.discovery.bfs.meta.Context;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author vector
 * @date 2023-11-11 21:34
 */
public class TestDF {
    public static void main(String[] args) {
        try (Context context = Global.getAppCtx().getBean(ContextService.class).load("test-local", "vector_data_service")) {
            // Create the database
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("StringColumn", DataTypes.StringType, false)
            });

            // 创建字符串列表
            List<String> stringList = Arrays.asList("String 9", "String 1", "String 1", "String 3", "String 1", "String 1");
            List<Row> rowList = stringList.stream()
                    .map(str -> RowFactory.create(str))
                    .collect(Collectors.toList());

            // 通过 Row 对象和模式创建 DataFrame
            Dataset<Row> stringDF = context.getSparkSession().createDataFrame(rowList, schema);
            JavaRDD<String> rdd1 = stringDF.map(new MapFunction<Row, String>() {
                private static final long serialVersionUID = 4172607015288917510L;

                @Override
                public String call(Row value) throws Exception {
                    return value.getString(0);
                }
            }, Encoders.STRING()).toJavaRDD();
            StructType schema2 = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("StringColumn2", DataTypes.StringType, false)
            });

            // 创建字符串列表
            List<String> stringList2 = Arrays.asList("String 3", "String 7", "String 1", "String 1", "String 1", "String 1");
            List<Row> rowList2 = stringList2.stream()
                    .map(str -> RowFactory.create(str))
                    .collect(Collectors.toList());

            // 通过 Row 对象和模式创建 DataFrame
            Dataset<Row> stringDF2 = context.getSparkSession().createDataFrame(rowList2, schema2);
            JavaRDD<String> rdd2 = stringDF2.map(new MapFunction<Row, String>() {

                private static final long serialVersionUID = -7164619704506809492L;

                @Override
                public String call(Row value) throws Exception {
                    return value.getString(0);
                }
            }, Encoders.STRING()).toJavaRDD();
            System.out.println("ret:" + rdd1.intersection(rdd2).collect());

        }
    }
}
