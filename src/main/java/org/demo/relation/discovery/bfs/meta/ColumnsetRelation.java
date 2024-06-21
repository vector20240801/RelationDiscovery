package org.demo.relation.discovery.bfs.meta;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author vector
 * @date 2023-08-09 00:27
 */
@Data
@NoArgsConstructor
public class ColumnsetRelation implements Serializable {
    private static final long serialVersionUID = 403002740627734205L;
    private String recordId;
    private String dataSource;
    private String tableNameA;

    private String columnsetA;
    private int columnsetASize;
    private String tablenameB;
    private String columnsetB;
    private int columnsetBsize;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnsetRelation that = (ColumnsetRelation) o;
        return tableNameA.equals(that.tableNameA) && columnsetA.equals(that.columnsetA) && tablenameB.equals(that.tablenameB) && columnsetB.equals(that.columnsetB);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableNameA, columnsetA, tablenameB, columnsetB);
    }
}
