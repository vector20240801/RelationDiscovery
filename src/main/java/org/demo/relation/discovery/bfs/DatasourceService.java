package org.demo.relation.discovery.bfs;


import com.alibaba.fastjson.JSON;
import com.google.common.hash.Hashing;
import org.demo.relation.discovery.bfs.filter.FilterService;
import org.demo.relation.discovery.bfs.loader.Loader;
import org.demo.relation.discovery.bfs.loader.LoaderType;
import org.demo.relation.discovery.bfs.loader.WholeDataSet;
import org.demo.relation.discovery.bfs.meta.ColumnsetRelation;
import org.demo.relation.discovery.bfs.meta.Context;
import org.demo.relation.discovery.bfs.util.FastUUID;
import org.demo.relation.discovery.bfs.util.MiscUtil;
import org.demo.relation.discovery.bfs.util.NamedThreadFactory;
import jodd.exception.UncheckedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Function1;
import scala.Tuple2;

import javax.annotation.Resource;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author vector
 * @date 2023-08-09 00:15
 */
@Service
public class DatasourceService implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DatasourceService.class);
    private static final long serialVersionUID = 2070334901363934477L;
    private static final StructType LINE_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("line_id", DataTypes.StringType, true),
            DataTypes.createStructField("table_name", DataTypes.StringType, true),
            DataTypes.createStructField("column_set", DataTypes.StringType, true),
            DataTypes.createStructField("column_set_size", DataTypes.IntegerType, true),
            DataTypes.createStructField("column_set_value", DataTypes.StringType, true),
            DataTypes.createStructField("create_time", DataTypes.DateType, true),
            DataTypes.createStructField("status", DataTypes.IntegerType, true),
            DataTypes.createStructField("datasource_name", DataTypes.StringType, true),
    });
    static final StructType RELATION_SCHEMA = DataTypes.createStructType(new StructField[]{
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
    @Value("${database.columnsetLineForId:vector_data_service.columnset_line}")
    private String columnsetLineForId;
    @Value("${database.columnsetRelation:vector_data_service.columnset_relation}")
    private String columnsetRelations;
    @Resource
    private FilterService filterService;
    @Resource
    private transient List<Loader> loaderList;

    public void createTable(Context context) {
        context.getSparkSession().sql("create DATABASE IF NOT EXISTS vector_fieldbfs");
        context.getSparkSession().sql("CREATE TABLE IF NOT EXISTS vector_fieldbfs.columnset_relation (\n" +
                "  relation_id string COMMENT 'Unique ID for the relationship',\n" +
                "\n" +
                "  table_name_a string COMMENT 'Table name of set A',\n" +
                "  column_set_a string COMMENT 'Columns that constitute set A',\n" +
                "  column_set_a_size int COMMENT 'Size of column set A',\n" +
                "  table_name_b string COMMENT 'Table name of set B',\n" +
                "  column_set_b string COMMENT 'Columns that constitute set B',\n" +
                "  column_set_b_size int COMMENT 'Size of column set B',\n" +
                "  create_time string COMMENT 'Creation time',\n" +
                "  status int COMMENT 'status'\n" +
                ")\n" +
                "PARTITIONED BY (datasource_name string)\n" +
                "STORED AS ORC;");
        context.getSparkSession().sql("CREATE TABLE IF NOT EXISTS vector_fieldbfs.columnset_line  (\n" +
                "   line_id string COMMENT 'Line ID',\n" +
                "   table_name string COMMENT 'Table name',\n" +
                "   column_set string COMMENT 'Column set',\n" +
                "   column_set_size int COMMENT 'Column set size',\n" +
                "   column_set_value string COMMENT 'Column value set separated by #',\n" +
                "   create_time string COMMENT 'Creation time',\n" +
                "   status int COMMENT 'status'\n" +
                ")\n" +
                "    PARTITIONED BY (datasource_name string)\n" +
                "    STORED AS ORC;");
        context.getSparkSession().sql("CREATE TABLE IF NOT EXISTS vector_fieldbfs.cache_columnset_relation (\n" +
                "   encoded_relation string COMMENT 'Column set',\n" +
                "   deleted int COMMENT 'Column set size',\n" +
                "   create_time string COMMENT 'Creation time'\n" +
                ")\n" +
                "    PARTITIONED BY (datasource_name string)\n" +
                "    STORED AS ORC;");
    }

    public void reloadTableRelation(Context context) {
        Loader loader = queryLoader(context.getDataSourceConfiguration().getLoaderType());
        clean(context, columnsetLineForId);
        clean(context, columnsetRelations);
        try {
            WholeDataSet wholeDataSet = loader.load(context);
            compressIdValue(context, wholeDataSet);
        } catch (Throwable e) {
            throw new UncheckedException("load compressed value error", e);
        }
        calculateRelationAndSave(context);
    }

    void clean(Context context, String tableName) {
        if ("test".equalsIgnoreCase(context.getEnv())) {
            return;
        }
        String deleteStatement = String.format("ALTER TABLE %s DROP IF EXISTS PARTITION (datasource_name='%s')",
                tableName, context.getDataSourceConfiguration().getDatasourceName());

        // Execute the SQL command to delete the partition
        context.getSparkSession().sql(deleteStatement);

    }

    private void calculateRelationAndSave(Context context) {

        Date createDate = new Date(System.currentTimeMillis());
        logger.info("[calculateRelationAndSave] {}", context.getDataSourceConfiguration().getTempeoryCompressedRowTable());
        Dataset<Row> compressedId = context.getSparkSession().sql(String.format("select * from %s where datasource_name='%s'",
                columnsetLineForId, context.getDataSourceConfiguration().getDatasourceName()));
        JavaRDD<ColumnsetRelation> rawRelation = compressedId.toJavaRDD().mapToPair(row -> new Tuple2<>((String) row.getAs("column_set_value"),
                        row.getAs("table_name") + "\t" + row.getAs("column_set") + "\t" + row.getAs("column_set_size"))).
                groupByKey().
                flatMapValues(new ConvertToRelation(context.getDataSourceConfiguration().getDatasourceName()
                )).values().distinct();

        JavaRDD<Row> dline = rawRelation.map(new RelationToRow(createDate));
        context.getSparkSession().createDataFrame(dline, RELATION_SCHEMA).write().mode(SaveMode.Append).insertInto(columnsetRelations);
    }


    private void compressIdValue(Context context, WholeDataSet wholeDataSet) throws ExecutionException, InterruptedException {
        ExecutorService workderexecutorService = new ThreadPoolExecutor(context.getDataSourceConfiguration().getLoaderWorkerCount() << 1,
                context.getDataSourceConfiguration().getLoaderWorkerCount() << 1,
                1, TimeUnit.SECONDS, new LinkedBlockingDeque<>(), new ThreadPoolExecutor.AbortPolicy()
        );
        // find idfield
        final ExecutorService executorService =
                new ThreadPoolExecutor(context.getDataSourceConfiguration().getLoaderWorkerCount(),
                        context.getDataSourceConfiguration().getLoaderWorkerCount(),
                        1, TimeUnit.SECONDS, new LinkedBlockingDeque<>(), new NamedThreadFactory("compressIdValue"),
                        new ThreadPoolExecutor.AbortPolicy());
        context.getDataSourceConfiguration().setTempeoryCompressedRowTable(FastUUID.next());
        List<Map.Entry<String, Dataset<Row>>> datasets =
                new ArrayList<>(wholeDataSet.getDatasetMap().entrySet());
        try {
            int batchlimit = context.getDataSourceConfiguration().getLoaderWorkerCount();
            int start = 0;
            int size = datasets.size();
            int end;
            for (; start < size; ) {
                end = Math.min(start + batchlimit, size);
                List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
                for (Map.Entry<String, Dataset<Row>> rowDataset : datasets.subList(start, end)) {
                    completableFutures.add(CompletableFuture.runAsync(() -> doCompressIdValue(context, rowDataset.getKey(),
                            rowDataset.getValue(), workderexecutorService), executorService));
                }
                for (CompletableFuture<Void> completableFuture : completableFutures) {
                    try {
                        completableFuture.get();
                    } catch (Throwable e) {
                        throw new UncheckedException("solveError", e);
                    }
                }
                start = end;
            }

        } finally {
            workderexecutorService.shutdown();
            executorService.shutdown();
        }
    }

    private Dataset<String> sample(Context context, Dataset<String> originSet) {
        JavaRDD<String> rdd = originSet.toJavaRDD().
                sortBy(new Function<String, Long>() {
                    private static final long serialVersionUID = -5376739264191869003L;

                    @Override
                    public Long call(String v1) {
                        return Hashing.murmur3_128(1).hashBytes(v1.getBytes(StandardCharsets.UTF_8)).asLong();
                    }
                }, true, 1).zipWithIndex()
                .filter(tuple -> tuple._2() < context.getDataSourceConfiguration().getSampleCount())
                .map(tuple -> tuple._1());
        return context.getSparkSession().createDataset(rdd.rdd(),
                Encoders.STRING());
    }

    private void doCompressIdValue(Context context, String tableName, Dataset<Row> originlines,
                                   ExecutorService executorService) {
        List<String> columnNames = new ArrayList<>();
        for (StructField field : originlines.schema().fields()) {
            if (StringUtils.containsAnyIgnoreCase(field.name(), context.getDataSourceConfiguration().getBackIdColumns())) {
                continue;
            }
            if (field.dataType() instanceof DatetimeType
                    || field.dataType() instanceof DecimalType
                    || field.dataType() instanceof DoubleType
                    || field.dataType() instanceof BooleanType) {
                continue;
            }
            columnNames.add(field.name());
        }
        Dataset<Row> lines = originlines.select(columnNames.stream().map(Column::new).toArray(Column[]::new)).persist(StorageLevel.MEMORY_AND_DISK_2());
        long count = lines.count();
        int deep = 1;
        Set<String> usedColumns = ConcurrentHashMap.newKeySet();

        Dataset<Row> compressIdsampleRow =
                context.getSparkSession().createDataFrame(Collections.emptyList(),
                        LINE_SCHEMA);
        int minIdLen = context.getDataSourceConfiguration().getMinIdLen();

        while (deep <= context.getDataSourceConfiguration().getColumnSetSize()) {
            Set<Set<String>> columnsSet = MiscUtil.queryAllCombination(columnNames,
                    usedColumns, deep);
            List<Future<Dataset<Row>>> list = new ArrayList<>();
            for (Set<String> columns : columnsSet) {
                Future<Dataset<Row>> future = CompletableFuture.supplyAsync(() -> {
                    Dataset<String> dataset = sample(context, lines.map(new CompressedValue(context, tableName, columns,
                                    this), Encoders.STRING()).
                            filter(new FilterFunction<String>() {
                                private static final long serialVersionUID = -8097302705953481016L;

                                @Override
                                public boolean call(String value) {
                                    return StringUtils.length(value) >= minIdLen;
                                }
                            }).distinct()).persist(StorageLevel.DISK_ONLY_3());
                    try {
                        long matchCount = dataset.count();
                        if (matchCount == 0) {
                            return null;
                        }
                        // not matched
                        if (count > context.getDataSourceConfiguration().getSampleCount() && 10000 / matchCount > context.getDataSourceConfiguration().getMatchScore()) {
                            return null;
                        }
                        usedColumns.addAll(columns);
                        return convertToRow(context, tableName, columns,
                                /*
                                 * limit size
                                 */
                                dataset).persist(StorageLevel.DISK_ONLY_3());
                    } finally {
                        dataset.rdd().unpersist(true);
                        dataset.unpersist();
                    }
                }, executorService);
                list.add(future);
            }
            try {
                for (Future<Dataset<Row>> future : list) {
                    Dataset<Row> sampleDataSet = future.get();
                    if (sampleDataSet == null) {
                        continue;
                    }
                    compressIdsampleRow = compressIdsampleRow.unionAll(sampleDataSet);
                    sampleDataSet.rdd().unpersist(true);
                    sampleDataSet.unpersist();
                }
            } catch (Throwable e) {
                throw new UncheckedException("can not compressed value", e);
            }
            ++deep;
        }
        synchronized (this) {
            compressIdsampleRow.write().mode(SaveMode.Append).insertInto(columnsetLineForId);
        }
        lines.rdd().unpersist(true);
        lines.unpersist();
        compressIdsampleRow.rdd().unpersist(true);
        compressIdsampleRow.unpersist();
    }

    public String encodeColumns(Set<String> columnsNames) {
        return JSON.toJSONString(columnsNames);
    }

    public String compressedValue(Context context, String tableName, Row row, Set<String> columnsNames) {
        Set<Object> valueSet = new HashSet<>();
        for (String column : columnsNames) {
            valueSet.add(filterService.filter(context.getDataSourceConfiguration(), tableName, column,
                    row.getAs(column)));
        }
        for (Object o : valueSet) {
            if (StringUtils.length(String.valueOf(o)) < context.getDataSourceConfiguration().getMinIdLen()) {
                return "";
            }
            if (StringUtils.containsAnyIgnoreCase(String.valueOf(o), "true", "false")) {
                return "";
            }
        }
        return StringUtils.join(valueSet, "$$TJE$$");
    }

    private Dataset<Row> convertToRow(Context context, String tableName,
                                      Set<String> rawColumnset, Dataset<String> compressedValue) {

        String columSet = encodeColumns(rawColumnset);
        JavaRDD<Row> rowJavaRDD = compressedValue.map(new ConvertToRow(context.getDataSourceConfiguration().getDatasourceName(),
                rawColumnset, columSet, tableName), Encoders.bean(Row.class)).toJavaRDD();
        return context.getSparkSession().createDataFrame(rowJavaRDD,
                LINE_SCHEMA);
    }


    private Loader queryLoader(LoaderType loaderType) {
        for (Loader loader : loaderList) {
            if (loaderType == loader.type()) {
                return loader;
            }
        }
        throw new IllegalArgumentException("loader type not exist");
    }

    public static class RelationToRow implements Function<ColumnsetRelation, Row>, Serializable {

        private static final long serialVersionUID = -7568549622968772578L;
        private final Date date;

        public RelationToRow(Date date) {
            this.date = date;
        }

        @Override
        public Row call(ColumnsetRelation item) throws Exception {
            return RowFactory.create(FastUUID.next(),
                    item.getTableNameA(), item.getColumnsetA(),
                    item.getColumnsetASize(), item.getTablenameB(),
                    item.getColumnsetB(),
                    item.getColumnsetBsize(), date, 1, item.getDataSource());
        }
    }


    public static class ConvertToRow implements Function1<String, Row>, Serializable {

        private static final long serialVersionUID = -6251958183363421881L;

        private final String datasourceName;
        private final Set<String> rawColumnset;
        private final String columSet;
        private final String tableName;

        public ConvertToRow(String datasourceName, Set<String> rawColumnset, String columSet, String tableName) {
            this.datasourceName = datasourceName;
            this.rawColumnset = rawColumnset;
            this.columSet = columSet;
            this.tableName = tableName;
        }

        @Override
        public Row apply(String item) {
            return RowFactory.create(FastUUID.next(),
                    tableName, columSet, rawColumnset.size(), item, new Date(System.currentTimeMillis()), 1, datasourceName);
        }
    }

    public static class ConvertToRelation implements FlatMapFunction<Iterable<String>, ColumnsetRelation>, Serializable {
        private static final long serialVersionUID = 223818204091785111L;
        private final String datasourceName;

        public ConvertToRelation(String datasourceName) {
            this.datasourceName = datasourceName;
        }

        @Override
        public Iterator<ColumnsetRelation> call(Iterable<String> rawstrings) {
            Set<String> rawSet = new HashSet<>();
            for (String rawstring : rawstrings) {
                rawSet.add(rawstring);
            }
            List<String> tableNameList = new ArrayList<>(rawSet);
            Set<Set<String>> combines = MiscUtil.queryAllCombination(tableNameList, Collections.emptySet(), 2);
            if (combines.isEmpty()) {
                return Collections.emptyIterator();
            }
            List<ColumnsetRelation> columnsetRelations = new ArrayList<>();
            for (Set<String> combine : combines) {
                if (combine.size() != 2) {
                    continue;
                }
                List<String> relations = new ArrayList<>(combine);


                /*
                 * Replace characters with 'tablea' if they are lexicographically smaller, and with 'tableb' if they are lexicographically greater.
                 */
                String aRow;
                String bRow;
                if (StringUtils.compare(relations.get(0),
                        relations.get(1)) > 0) {
                    aRow = relations.get(0);
                    bRow = relations.get(1);
                } else {
                    aRow = relations.get(1);
                    bRow = relations.get(0);
                }
                String[] aInfo = StringUtils.split(aRow, "\t");
                String[] bInfo = StringUtils.split(bRow, "\t");

                if (StringUtils.equals(aInfo[0], bInfo[0])) {
                    continue;
                }

                ColumnsetRelation columnsetRelation = new ColumnsetRelation();
                columnsetRelation.setDataSource(datasourceName);
                columnsetRelation.setTableNameA(aInfo[0]);
                columnsetRelation.setColumnsetA(aInfo[1]);
                columnsetRelation.setColumnsetASize(Integer.parseInt(aInfo[2]));
                columnsetRelation.setTablenameB(bInfo[0]);
                columnsetRelation.setColumnsetB(bInfo[1]);
                columnsetRelation.setColumnsetBsize(Integer.parseInt(bInfo[2]));

                columnsetRelations.add(columnsetRelation);
            }
            return columnsetRelations.iterator();
        }
    }

    public static class CompressedValue implements Function1<Row, String>, Serializable {
        private static final long serialVersionUID = 1380163185147192062L;
        private final Context context;
        private final String tableName;
        private final Set<String> columns;
        private final DatasourceService datasourceService;

        public CompressedValue(Context context, String tableName, Set<String> columnsNames, DatasourceService datasourceService) {
            this.context = context;
            this.tableName = tableName;
            this.columns = columnsNames;
            this.datasourceService = datasourceService;
        }

        @Override
        public String apply(Row item) {
            return datasourceService.compressedValue(context, tableName, item, columns);
        }
    }
}
