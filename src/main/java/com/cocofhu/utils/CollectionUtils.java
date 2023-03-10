package com.cocofhu.utils;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class CollectionUtils {
    public static <K> void notExistKeys(Set<K> set, K[] keys, Consumer<K> consumer){
        Arrays.stream(keys).forEach(key->{
            if(!set.contains(key)){
                consumer.accept(key);
            }
        });
    }
    public static <K,V> void notExistKeys(Map<K,V> map, K[] keys, Consumer<K> consumer){
        notExistKeys(map.keySet(),keys,consumer);
    }
}
