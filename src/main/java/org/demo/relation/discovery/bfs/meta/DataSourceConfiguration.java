package org.demo.relation.discovery.bfs.meta;

import org.demo.relation.discovery.bfs.filter.FilterItem;
import org.demo.relation.discovery.bfs.loader.LoaderType;
import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author vector
 * @date 2023-08-09 01:13
 */
@Data
public class DataSourceConfiguration implements Serializable {
    private static final long serialVersionUID = -3511690718167850742L;
    private LoaderType loaderType;
    private String datasourceName;
    /**
     * optional you can find it from guangfu
     */
    private String datasourceNameForVector;
    private String profile;
    /**
     * sample count for compressedValue default=1000
     */
    private int sampleCount = 5000;
    /**
     * default=2
     */
    private int columnSetSize = 2;
    /**
     * deault=100
     */
    private long matchScore = 10;
    /**
     * default=40
     */
    private int matchedPercent = 100;
    private Set<String> idColumnTag = Collections.emptySet();
    private String[] backIdColumns;
    /**
     * the number of worker thread used for load data from hive
     * defalut is 128
     */
    private int loaderWorkerCount = 128;
    private int minIdLen = 5;

    /*
     * compressedTemorary
     */
    private String tempeoryCompressedRowTable;
    private List<FilterItem> filters;
}
