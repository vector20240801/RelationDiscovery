package org.demo.relation.discovery.bfs.filter;

import java.io.Serializable;

/**
 * @author vector
 * @date 2023-08-12 13:58
 */
public interface ValueFilter extends Serializable {

    /**
     * @param columnName  columnName
     * @param originValue originValue
     * @return currentValue
     */
    Object filter(String columnName, Object originValue);
}
