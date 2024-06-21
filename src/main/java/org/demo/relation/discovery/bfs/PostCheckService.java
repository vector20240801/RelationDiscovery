package org.demo.relation.discovery.bfs;

import com.alibaba.fastjson.JSON;
import org.demo.relation.discovery.bfs.meta.CheckInCacheResult;
import org.demo.relation.discovery.bfs.meta.Context;
import org.demo.relation.discovery.bfs.meta.EncodedRelation;
import org.demo.relation.discovery.bfs.util.MiscUtil;
import jodd.exception.UncheckedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author vector
 * @date 2023-11-05 16:57
 */
@Service
public class PostCheckService {
    @Value("${database.columnsetLineForId:vector_data_service.columnset_line}")
    private String columnsetLineForId;
    @Value("${database.columnsetRelation:vector_data_service.columnset_relation}")
    private String columnsetRelations;
    @Value("${database.cache.columnsetRelation:vector_data_service.cache_columnset_relation}")
    private String cacahedColumnsetRelations;
    @Resource
    private JdbcTemplate jdbcTemplate;
    private static final StructType CACHED_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("encoded_relation", DataTypes.StringType, true),
            DataTypes.createStructField("deleted", DataTypes.IntegerType, true),
            DataTypes.createStructField("create_time", DataTypes.DateType, true),
            DataTypes.createStructField("datasource_name", DataTypes.StringType, true)
    });

    private Set<String> checkRelations(Context context, List<Map<String, Object>> list) {
        if (list.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> columnsSet = list.stream().map(item -> (String) item.get("column_set_a")).collect(Collectors.toSet());
        columnsSet.addAll(list.stream().map(item -> (String) item.get("column_set_b")).collect(Collectors.toSet()));
        Set<String> tableSet = list.stream().map(item -> (String) item.get("table_name_a")).collect(Collectors.toSet());
        tableSet.addAll(list.stream().map(item -> (String) item.get("table_name_b")).collect(Collectors.toSet()));
        List<Map<String, Object>> valuesetList = jdbcTemplate.queryForList(String.format("select * from %s where datasource_name='%s' and " +
                        "table_name in (%s) and column_set in (%s)", columnsetLineForId, context.getDataSourceConfiguration().getDatasourceName(),
                StringUtils.join(tableSet.stream().map(item -> "'" + item + "'").toArray(), ","), StringUtils.join(columnsSet.stream().map(item -> "'" + item + "'").toArray(), ",")));
        Map<String, Set<String>> valueMap = new HashMap<>(valuesetList.size());
        for (Map<String, Object> map : valuesetList) {
            String setkey = String.format("%s-%s",
                    map.get("table_name"), map.get("column_set"));
            String setValue = (String) map.get("column_set_value");
            valueMap.computeIfAbsent(setkey,
                    key -> new HashSet<>()).add(setValue);
        }

        Set<String> deletedId = new HashSet<>();
        for (Map<String, Object> item : list) {
            String relationId = (String) item.get("relation_id");
            if (relationId == null) {
                continue;
            }
            deletedId.addAll(doCheckRelation(context, item));
        }
        return deletedId;
    }

    private Set<String> doCheckRelation(Context context, Map<String, Object> item) {
        String tableNameA = (String) item.get("table_name_a");
        String tableNameB = (String) item.get("table_name_b");
        List<String> columnSetAList = JSON.parseArray((String) item.get("column_set_a")).stream().map(Object::toString).collect(Collectors.toList());
        List<String> columnSetBList = JSON.parseArray((String) item.get("column_set_b")).stream().map(Object::toString).collect(Collectors.toList());
        System.out.println("[doCheckRelation] tableNameA:" + tableNameA + " columnSetAList:" + columnSetAList
                + " tableNameB:" + tableNameB + " columnSetBList:" + columnSetBList);
        if (columnSetAList.size() > 1 || columnSetBList.size() > 1) {
            return Collections.emptySet();
        }
        JavaRDD<String> rddA = context.getSparkSession().table(String.format("%s.%s",
                context.getDataSourceConfiguration().getDatasourceNameForVector(), tableNameA)).select(columnSetAList.get(0)).map(new StringMap(), Encoders.STRING()).filter(new EMPTYFilter()).distinct().toJavaRDD().persist(StorageLevel.DISK_ONLY_3());
        JavaRDD<String> rddB = context.getSparkSession().table(String.format("%s.%s",
                        context.getDataSourceConfiguration().getDatasourceNameForVector(), tableNameB)).select(columnSetBList.get(0)).
                map(new StringMap(), Encoders.STRING()).filter(new EMPTYFilter()).distinct().toJavaRDD().persist(StorageLevel.DISK_ONLY_3());

        JavaRDD<String> rddI = rddA.intersection(rddB);
        try {
            long countA = rddA.count();
            long countB = rddB.count();
            long countI = rddI.count();
            int matchedCount = context.getDataSourceConfiguration().getMatchedPercent();
            if (countI * 100 / countA < matchedCount
                    && countI * 100 / countB < matchedCount) {
                return Collections.singleton((String) item.get("relation_id"));
            } else {
                return Collections.emptySet();
            }
        } finally {
            rddA.unpersist();
            rddB.unpersist();
            rddI.unpersist();
        }
    }

    public void postCheckRelation(Context context) {
        ExecutorService executorService = new ThreadPoolExecutor(context.getDataSourceConfiguration().getLoaderWorkerCount() << 2,
                context.getDataSourceConfiguration().getLoaderWorkerCount() << 2,
                1, TimeUnit.SECONDS, new LinkedBlockingDeque<>(), new ThreadPoolExecutor.AbortPolicy()
        );
        System.out.println("[postCheckRelation] begin");
        Set<String> deletedId = new ConcurrentSkipListSet<>();
        Set<EncodedRelation> encodedRelations = new ConcurrentSkipListSet<>();
        try {
            AtomicLong atomicLong = new AtomicLong(0L);
            List<CompletableFuture<Void>> futureList = new ArrayList<>();
            List<Map<String, Object>>
                    relationIds = jdbcTemplate.queryForList(String.format("select relation_id from %s where datasource_name=?",
                    columnsetRelations), context.getDataSourceConfiguration().getDatasourceName());
            List<Map<String, Object>>
                    cachedList = jdbcTemplate.queryForList(String.format("select * from %s where datasource_name=?",
                    cacahedColumnsetRelations), context.getDataSourceConfiguration().getDatasourceName());

            Map<String, Boolean> cachedRelation = cachedList.stream().collect(Collectors.toMap(item -> (String) item.get("encoded_relation"), item -> "1".equals(
                    String.valueOf(item.get("deleted"))), (o1, o2) -> o1));

            MiscUtil.batchSolveNoReturn(relationIds, 8, partKeys -> {
                futureList.add(CompletableFuture.runAsync(() -> {
                    List<Map<String, Object>> originList = jdbcTemplate.queryForList(String.format("select * from %s where datasource_name='%s' and relation_id in (%s)",
                            columnsetRelations, context.getDataSourceConfiguration().getDatasourceName(), StringUtils.join(partKeys.stream().map(item -> "'" + item.get("relation_id") + "'").toArray(), ",")));
                    CheckInCacheResult checkInCacheResult = checkInCacheResult(originList, cachedRelation);
                    deletedId.addAll(checkInCacheResult.getDeleteIds());


                    Set<String> deleteIdSet = checkRelations(context, checkInCacheResult.getNeedToCheckResult());
                    for (Map<String, Object> item : checkInCacheResult.getNeedToCheckResult()) {
                        EncodedRelation encodedRelation = new EncodedRelation();
                        encodedRelation.setEncodedRelation(encodeRelation(item));

                        if (deleteIdSet.contains((String) item.get("relation_id"))) {
                            encodedRelation.setStatus(1);
                        } else {
                            encodedRelation.setStatus(0);
                        }
                        encodedRelations.add(encodedRelation);
                    }

                    deletedId.addAll(deleteIdSet);
                    atomicLong.addAndGet(checkInCacheResult.getNeedToCheckResult().size());
                    System.out.printf("[postCheckRelation]finish %d relations\n", atomicLong.get());
                }, executorService));
            });
            for (CompletableFuture<Void> future : futureList) {
                try {
                    future.get();
                } catch (Throwable e) {
                    throw new UncheckedException("post check error", e);
                }
            }

            System.out.println("[postCheckRelation] begin end deleted ids " + deletedId);
            Dataset<Row> dline = context.getSparkSession().sql(String.format("select * from %s where datasource_name='%s'",
                    columnsetRelations, context.getDataSourceConfiguration().getDatasourceName())).filter(
                    new IdFilterFunction(deletedId));
            System.out.println("[postCheckRelation] relation count" + dline.count());

            // save new relation
            if (!encodedRelations.isEmpty()) {
                int time = 0;
                while (++time < 10) {
                    try {
                        context.getSparkSession().createDataFrame(
                                        encodedRelations.stream().map(item -> RowFactory.create(item.getEncodedRelation(),
                                                item.getStatus(), new Date(System.currentTimeMillis()),
                                                context.getDataSourceConfiguration().getDatasourceName())).collect(Collectors.toList()), CACHED_SCHEMA).
                                write().mode(SaveMode.Append).insertInto(cacahedColumnsetRelations);
                        break;
                    } catch (Throwable e) {
                        System.err.println("log cache data error");
                        e.printStackTrace();
                    }
                }
            }


            dline.write().mode(SaveMode.Overwrite).insertInto(columnsetRelations);
        } finally {
            executorService.shutdown();
        }
    }

    private CheckInCacheResult checkInCacheResult(List<Map<String, Object>> originList,
                                                  Map<String, Boolean> cachedRelation) {
        CheckInCacheResult checkInCacheResult = new CheckInCacheResult();
        for (Map<String, Object> map : originList) {
            String code = encodeRelation(map);
            if (cachedRelation.get(code) == null) {
                checkInCacheResult.getNeedToCheckResult().add(map);
                continue;
            }
            if (cachedRelation.get(code)) {
                checkInCacheResult.getDeleteIds().add((String) map.get("relation_id"));
            }
        }
        return checkInCacheResult;
    }

    private String encodeRelation(Map<String, Object> item) {
        return String.format("%s-%s-%s-%s",
                item.get("table_name_a"), item.get("column_set_a"),
                item.get("table_name_b"), item.get("column_set_b"));
    }


    public static class EMPTYFilter implements FilterFunction<String> {

        private static final long serialVersionUID = 7370345380436024579L;

        @Override
        public boolean call(String value) throws Exception {
            return !StringUtils.isEmpty(value);
        }
    }

    public static class StringMap implements MapFunction<Row, String> {

        private static final long serialVersionUID = -7584518575784407248L;

        @Override
        public String call(Row value) throws Exception {
            return String.valueOf(value.get(0));
        }
    }

    public static class IdFilterFunction implements FilterFunction<Row> {

        private static final long serialVersionUID = -1466787307618378148L;
        private final Set<String> deletedId;

        public IdFilterFunction(Set<String> deletedId) {
            this.deletedId = deletedId;
        }


        @Override
        public boolean call(Row value) {
            return !deletedId.contains((String) value.getAs("relation_id"));
        }
    }
}
