package com.test.demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.List;

/**
 * @author vector
 * @date 2023-09-05 13:13
 */
public class TestIndex {
    public static void main(String[] args) {
//        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition", "true")
//                .config("hive.exec.dynamic.partition.mode", "nonstrict")
//                .appName("My Spark Application " + System.currentTimeMillis())
//                .master("local[*]")
//                .getOrCreate();
//        try {
//            List<String> list = new ArrayList<>();
//            for (int i = 0; i < 100; i++) {
//                list.add("abc" + i);
//            }
//            // 传递类标签
//            ClassTag<String> tag = ClassTag$.MODULE$.apply(String.class);
//            JavaRDD<String> rdd = sparkSession.sparkContext().parallelize(
//                    JavaConverters.asScalaBuffer(list).toSeq(), 1, tag).toJavaRDD();
//            System.out.println(rdd.zipWithIndex().filter(tuple -> {
//                return tuple._2 < 10L;
//            }).map(item -> {
//                return item._1;
//            }).collect());
//        } finally {
//            sparkSession.close();
//        }
    }
}
