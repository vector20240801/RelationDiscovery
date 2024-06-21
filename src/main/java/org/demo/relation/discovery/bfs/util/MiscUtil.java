package org.demo.relation.discovery.bfs.util;

import java.util.*;
import java.util.function.Consumer;

/**
 * @author vector
 * @date 2023-08-11 20:43
 */
public class MiscUtil {
    public static <T> List<T> iteratorToList(Iterator<T> iterator) {
        List<T> list = new ArrayList<>();
        while (iterator.hasNext()) {
            T element = iterator.next();
            list.add(element);
        }
        return list;
    }

    public static <K> void batchSolveNoReturn(List<K> params, int batchlimit, Consumer<List<K>> consumer) {
        if (batchlimit <= 0) throw new IllegalArgumentException("the batch limit must be greater than zero!");

        if (params.size() < batchlimit) {
            consumer.accept(params);
        } else {
            int start = 0;
            int size = params.size();
            int end;
            for (; start < size; ) {
                end = start + batchlimit < size ? start + batchlimit : size;
                consumer.accept(params.subList(start, end));
                start = end;
            }

        }
    }


    public static <T> Set<Set<T>> queryAllCombination(List<T> colmunsName,
                                                      Set<T> blackColumns, int deep) {
        Set<Set<T>> result = new HashSet<>(1);
        doQueryAllCombination(colmunsName, blackColumns, 0, deep, new int[deep], result);
        return result;
    }

    private static <T> void doQueryAllCombination(List<T> colmunsName,
                                                  Set<T> blackColumns, int index, int deep, int[] arrayIndex,
                                                  Set<Set<T>> currentSet) {
        if (deep == 0) {
            Set<T> selectedColumnsSet = new HashSet<>(1);
            for (int i : arrayIndex) {
                selectedColumnsSet.add(colmunsName.get(i));
            }
            currentSet.add(selectedColumnsSet);
            return;
        }
        for (int i = index; i < colmunsName.size(); i++) {
            if (blackColumns.contains(colmunsName.get(i))) {
                continue;
            }
            arrayIndex[deep - 1] = i;
            doQueryAllCombination(colmunsName, blackColumns, i + 1, deep - 1, arrayIndex, currentSet);
        }
    }
}
