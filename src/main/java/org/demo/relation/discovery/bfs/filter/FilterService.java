package org.demo.relation.discovery.bfs.filter;

import org.demo.relation.discovery.bfs.meta.DataSourceConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * @author vector
 * @date 2023-08-12 14:53
 */
@Service
public class FilterService implements Serializable {
    private static final long serialVersionUID = 2836300657287387648L;
    private Map<String, String> cache = new ConcurrentHashMap<>();
    private final static String EMTPY = "none";
    @Resource
    private Map<String, ValueFilter> valueFilterMap;


    public Object filter(DataSourceConfiguration dataSourceConfiguration, String tableName, String columnName, Object originValue) {
        if (dataSourceConfiguration.getFilters() == null) {
            return originValue;
        }
        String fitler = queryFilterName(dataSourceConfiguration, tableName, columnName);
        if (!StringUtils.equals(EMTPY, fitler)) {
            return valueFilterMap.get(fitler).filter(
                    columnName, originValue);
        } else {
            return originValue;
        }
    }

    private String queryFilterName(DataSourceConfiguration configuration, String tableName, String columnName) {
        return cache.computeIfAbsent(String.format("%s-%s-%s", configuration.getDatasourceName(),
                tableName, columnName), key -> {
            for (FilterItem filter : configuration.getFilters()) {
                if (Pattern.compile(filter.getColumnPattern(), Pattern.DOTALL).matcher(columnName).find()) {
                    return filter.getFilter();
                }
            }
            return EMTPY;
        });
    }
}
