package org.demo.relation.discovery.bfs.meta;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author vector
 * @date 2023-08-09 00:28
 */
@Data
@NoArgsConstructor
public class ColumnsetValue implements Serializable {

    private static final long serialVersionUID = -2159936773079653918L;
    private String recordId;

    private String tableName;
    private String columnset;

    private String columnsetValue;
    private int columnsetSize;
    private java.sql.Date createTime;
}
