package org.demo.relation.discovery.bfs.meta;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

/**
 * @author vector
 * @date 2023-11-24 22:52
 */
@Data
public class EncodedRelation implements Comparable<EncodedRelation> {
    private String encodedRelation;
    private int status;

    @Override
    public int compareTo(@NotNull EncodedRelation o) {
        return encodedRelation.compareTo(o.encodedRelation);
    }

}
