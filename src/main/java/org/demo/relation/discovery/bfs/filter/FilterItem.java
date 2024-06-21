package org.demo.relation.discovery.bfs.filter;

import lombok.Data;

import java.io.Serializable;

/**
 * @author vector
 * @date 2023-08-12 14:55
 */
@Data
public class FilterItem implements Serializable {
    private static final long serialVersionUID = -1863198225814122597L;
    private String columnPattern;
    private String filter;
}
