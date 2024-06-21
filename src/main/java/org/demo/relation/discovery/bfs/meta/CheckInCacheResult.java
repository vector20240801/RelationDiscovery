package org.demo.relation.discovery.bfs.meta;

import lombok.Data;

import java.util.*;

/**
 * @author vector
 * @date 2023-11-24 22:52
 */
@Data
public class CheckInCacheResult {
    private Set<String> deleteIds = new HashSet<>();
    private List<Map<String, Object>> needToCheckResult = new ArrayList<>();
}
