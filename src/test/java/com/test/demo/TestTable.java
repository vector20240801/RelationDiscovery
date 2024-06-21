package com.test.demo;

import org.demo.relation.discovery.bfs.meta.ColumnsetRelation;
import org.demo.relation.discovery.bfs.util.FastUUID;
import jodd.util.StringUtil;
import lombok.Data;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.util.*;

/**
 * @author vector
 * @date 2023-08-15 22:45
 */
public class TestTable {
    private static final StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("a", DataTypes.StringType, true),
            DataTypes.createStructField("b", DataTypes.StringType, true),
            DataTypes.createStructField("c", DataTypes.StringType, true),
            DataTypes.createStructField("d", DataTypes.IntegerType, true)
    });

    public static void main(String[] args) throws IOException {

        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .appName("My Spark Application " + System.currentTimeMillis())
                .master("local[*]")
                .getOrCreate();
        try {

            Date date = new Date(System.currentTimeMillis());
            // 创建要插入的数据行
            Row row1 = RowFactory.create(
                    "line1",
                    "table1",
                    "column_set1",
                    5,
                    "column_set_value1",
                    date,
                    1,
                    "test-local" // 添加一个假的静态分区列
            );
            Row row2 = RowFactory.create(
                    "line1",
                    "table2",
                    "column_set1",
                    5,
                    "column_set_value1",
                    date,
                    1,
                    "test-local" // 添加一个假的静态分区列
            );
            Row row3 = RowFactory.create(
                    "line1",
                    "table1",
                    "column_set1",
                    5,
                    "column_set_value2",
                    date,
                    1,
                    "test-local" // 添加一个假的静态分区列
            );
            List<Row> rowList1 = new ArrayList<>();
            rowList1.add(row1);
            List<Row> rowList2 = new ArrayList<>();
            rowList2.add(row3);
            rowList2.add(row2);

            // 定义表名
            String tableName = "vector_data_service.columnset_line";

            String deleteStatement = String.format("ALTER TABLE %s DROP IF EXISTS PARTITION (datasource_name='%s')",
                    tableName, "test-local");

            // Execute the SQL command to delete the partition
            sparkSession.sql(deleteStatement);

            // 创建Schema，与表结构保持一致
            StructType schema = new StructType()
                    .add("line_id", DataTypes.StringType)
                    .add("table_name", DataTypes.StringType)
                    .add("column_set", DataTypes.StringType)
                    .add("column_set_size", DataTypes.IntegerType)
                    .add("column_set_value", DataTypes.StringType)
                    .add("create_time", DataTypes.DateType)
                    .add("status", DataTypes.IntegerType)
                    .add("datasource_name", DataTypes.StringType); // 添加假的静态分区列
            List<ColumnsetRelation> columnsetRelations = new ArrayList<>();
            ColumnsetRelation columnsetRelation1 = new ColumnsetRelation();
            columnsetRelation1.setDataSource("a");
            columnsetRelation1.setTableNameA("a");
            columnsetRelation1.setColumnsetA("a");
            columnsetRelation1.setColumnsetASize(1);
            columnsetRelation1.setTablenameB("b");
            columnsetRelation1.setColumnsetB("b");
            columnsetRelation1.setColumnsetBsize(1);
            ColumnsetRelation columnsetRelation2 = new ColumnsetRelation();
            columnsetRelation2.setDataSource("a");
            columnsetRelation2.setTableNameA("a");
            columnsetRelation2.setColumnsetA("a");
            columnsetRelation2.setColumnsetASize(1);
            columnsetRelation2.setTablenameB("b");
            columnsetRelation2.setColumnsetB("b");
            columnsetRelation2.setColumnsetBsize(1);
            columnsetRelations.add(columnsetRelation1);
            columnsetRelations.add(columnsetRelation2);
            sparkSession.createDataset(columnsetRelations, Encoders.bean(ColumnsetRelation.class)).distinct().show(10);


//            Dataset<Row> dataset = sparkSession.createDataFrame(Collections.emptyList(),
//                    schema);
//            List<String> list = new ArrayList<>();
//            list.add("a");
//            list.add("b");
//            // 创建DataFrame
//            dataset = dataset.unionAll(sparkSession.createDataFrame(rowList1, schema));
//            dataset = dataset.unionAll(sparkSession.createDataFrame(rowList2, schema));
//            JavaPairRDD<String, Iterable<String>> groupedData = dataset.toJavaRDD()
//                    .mapToPair(row -> new Tuple2<>((String) row.getAs("column_set_value"), row.getAs("table_name") + "\t" + row.getAs("column_set") + "\t" + row.getAs("column_set_size"))).groupByKey();
//            long count = groupedData.flatMapValues(new MapIterator()).count();
//            System.out.println(count);

//            Dataset<Row> groupedData  = dataset.toJavaRDD().groupBy(new Column("column_set_value")).agg(functions.collect_list("table_name").as("dataAList"));
//            groupedData.flatMap(new MapIterator(),
//                    Encoders.bean(Row.class)).count();
//            dataset.show(10);
        } finally {
            sparkSession.close();
        }
    }

    public static class MapIterator implements FlatMapFunction<Iterable<String>, Row>, Serializable {

        private static final long serialVersionUID = -5890427681058478927L;


        @Override
        public Iterator<Row> call(Iterable<String> s) throws Exception {
            Set<String> list = new HashSet<>();
            s.forEach(list::add);
            System.out.println(s);
            return Collections.singletonList(RowFactory.create(StringUtil.join(list, ","))).iterator();
        }
    }

    public static class ConvertToRow implements Function1<String, Row>, Serializable {

        private static final long serialVersionUID = -6251958183363421881L;

        @Override
        public Row apply(String item) {

            Row row = new GenericRowWithSchema(new Object[]{FastUUID.next(),
                    "abc",
                    item, 1},
                    schema);
            return row;
        }
    }

    @Data
    public static class Item {
        private final String a;
        private final String b;
        private final String c;
        private final int d;
    }
}
