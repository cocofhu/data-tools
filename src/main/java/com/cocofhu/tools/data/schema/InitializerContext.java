package com.cocofhu.tools.data.schema;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InitializerContext {
    // SchemaPlus for jdbc initialization
    public static final String PARENT_SCHEMA = "PARENT_SCHEMA";
    private final Map<String,Object> attribute = new HashMap<>();
    private final List<Closeable> closeableList = new ArrayList<>();
    // 当引擎关闭时 释放资源
    public void addCloseable(Closeable closeable){
        closeableList.add(closeable);
    }
    public void setAttribute(String key,Object value){
        attribute.put(key,value);
    }
    public Object getAttribute(String key){
        return attribute.get(key);
    }
    public void close(){
        closeableList.forEach(closeable -> {
            try {
                closeable.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
