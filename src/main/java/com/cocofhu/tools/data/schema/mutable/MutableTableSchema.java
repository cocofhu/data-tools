package com.cocofhu.tools.data.schema.mutable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class MutableTableSchema extends AbstractSchema {

    private final Map<String,Table> tableMap = new HashMap<>();

    public void putNewTable(String name,Table table){
        tableMap.put(name,table);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }
}
