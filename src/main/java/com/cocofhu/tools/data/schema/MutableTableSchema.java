package com.cocofhu.tools.data.schema;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class MutableTableSchema extends AbstractSchema {

    private final Map<String,Table> tableMap = new HashMap<>();

    public MutableTableSchema putNewTable(String name,Table table){
        tableMap.put(name,table);
        return this;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }
}
