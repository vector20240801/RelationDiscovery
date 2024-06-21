package org.demo.relation.discovery.bfs;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.demo.relation.discovery.bfs.meta.Context;
import org.demo.relation.discovery.bfs.meta.DataSourceConfiguration;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.representer.Representer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * @author vector
 * @date 2023-08-09 01:45
 */
@Service
public class ContextService {
    @Value("${spark.url}")
    private String sparkUrl;
    @Value("${spark.sql.warehouse.dir}")
    private String wareHouse;
    @Value("${spark.memory:512M}")
    private String memory;
    @Value("${fieldbfs.env:test}")
    private String env;

    /**
     * - name: SPARK_CONFIG_JSON
     * value: |
     * {
     * "spark.master": "k8s://https://kubernetes.default.svc",
     * "spark.app.name": "vector Execution Service",
     * "spark.kubernetes.container.image": "882490700787.dkr.ecr.us-east-1.amazonaws.com/spark-executor:latest-main",
     * "spark.kubernetes.namespace": "services",
     * "spark.kubernetes.container.image.pullPolicy": "Always",
     * "spark.kubernetes.authenticate.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
     * "spark.kubernetes.authenticate.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
     * "spark.hadoop.fs.defaultFS": "hdfs://hadoop-namenode:9820",
     * "spark.hive.metastore.uris": "thrift://hive-metastore.data-tooling:9083",
     * "spark.driver.memory": "2g",
     * "spark.driver.bindAddress": "0.0.0.0",
     * "spark.driver.port": "9000",
     * "spark.executor.memory": "4g",
     * "spark.executor.pyspark.memory": "4g",
     * "spark.executor.cores": "1",
     * "spark.dynamicAllocation.shuffleTracking.enabled": "true",
     * "spark.dynamicAllocation.shuffleTracking.timeout": "1h",
     * "spark.dynamicAllocation.enabled": "true",
     * "spark.dynamicAllocation.minExecutors": "1",
     * "spark.dynamicAllocation.maxExecutors": "10",
     * "spark.dynamicAllocation.executorAllocationRatio": "1.0",
     * "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
     * "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout": "5s",
     * "spark.blockManager.port": "7077",
     * "spark.driver.maxResultSize": "0",
     * "spark.rpc.message.maxSize": "1536",
     * "spark.sql.warehouse.dir": "hdfs://hadoop-namenode:9820/user/hive/warehouse",
     * "spark.eventLog.enabled": "true",
     * "spark.eventLog.dir": "hdfs://hadoop-namenode:9820/user/C4/spark/eventLogs/"
     * }
     *
     * @param datasourceName
     * @param vectorDataSourceName
     * @return
     */
    public Context load(String datasourceName, String vectorDataSourceName) {
        System.out.printf("load datainfo %s %s %s\n", env, datasourceName, vectorDataSourceName);
        Context context = new Context();
        context.setEnv(env);
        InputStream stream = null;
        try {
            stream = getClass().getClassLoader().getResourceAsStream("bfs-datasource.yml");
            SparkSession sparkSession;
            if (StringUtils.equals("pro", env)) {
                sparkSession = SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition", "true")
                        .config("hive.exec.dynamic.partition.mode", "nonstrict").config("spark.cleaner.referenceTracking.cleanCheckpoints",
                                true).appName(String.format("table-relation-discover-%s-%s", datasourceName,
                                System.currentTimeMillis())).getOrCreate();
            } else {
                sparkSession = SparkSession.builder().enableHiveSupport().config("spark.sql.warehouse.dir",
                                wareHouse).config("hive.exec.dynamic.partition", "true")
                        .config("hive.exec.dynamic.partition.mode", "nonstrict")
                        .appName(String.format("table-relation-discover-%s-%s", datasourceName,
                                System.currentTimeMillis())).
                        config("spark.cleaner.referenceTracking.cleanCheckpoints",
                                true)
                        .master(sparkUrl).getOrCreate();
            }

            ConfigSet configSet = buildObjectV2(stream, ConfigSet.class);
            System.out.println(configSet.getConfigs());
            DataSourceConfiguration dataSourceConfiguration = null;
            for (DataSourceConfiguration config : configSet.getConfigs()) {
                if (StringUtils.equalsIgnoreCase("default", config.getProfile())) {
                    dataSourceConfiguration = config;
                }
                if (StringUtils.equalsIgnoreCase(datasourceName, config.getProfile())) {
                    dataSourceConfiguration = config;
                }
            }
            if (dataSourceConfiguration == null) {
                throw new IllegalArgumentException("config not exist");
            }


            context.setDataSourceConfiguration(dataSourceConfiguration);
            if (dataSourceConfiguration.getIdColumnTag() != null) {
                dataSourceConfiguration.setBackIdColumns(dataSourceConfiguration.getIdColumnTag().toArray(new String[dataSourceConfiguration.getIdColumnTag().size()]));
            }
            dataSourceConfiguration.setDatasourceName(datasourceName);
            dataSourceConfiguration.setDatasourceNameForVector(vectorDataSourceName);
            context.setSparkSession(sparkSession);
            return context;
        } catch (Throwable e) {
            context.close();
            throw new UncheckedExecutionException("load context error", e);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    throw new UncheckedExecutionException("close error", e);
                }
            }
        }

    }

    private static <T> T buildObjectV2(InputStream stream, Class<T> objectClass) {

        Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);
        Yaml yaml = new Yaml(new Constructor(objectClass), representer);
        T ret;
        try {
            ret = yaml.loadAs(stream, objectClass);
        } catch (YAMLException e) {
            throw new IllegalArgumentException("illegal config", e);
        }

        if (ret == null) {
            throw new IllegalArgumentException("config not exist");
        }
        return ret;
    }

    @Data
    public static class ConfigSet {
        private List<DataSourceConfiguration> configs;

        public ConfigSet() {
        }
    }


}
