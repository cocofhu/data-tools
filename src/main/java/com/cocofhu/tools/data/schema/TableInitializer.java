package com.cocofhu.tools.data.schema;


import com.cocofhu.tools.data.factory.TableDefinition;
import org.apache.calcite.schema.Table;

import java.util.Map;


public interface TableInitializer {
    Table initCurrent(TableDefinition tableDefinition, Map<String,Object> initParams);
    default Table initFully(TableDefinition tableDefinition, Map<String,Object> initParams){
        if(isCurrentInitializer(tableDefinition, initParams)){
            return initCurrent(tableDefinition,initParams);
        }else{
            try {
                Class<?> clazz = Class.forName(tableDefinition.getInitClass());
                TableInitializer initializer = (TableInitializer) clazz.newInstance();
                return initializer.initFully(tableDefinition,initParams);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new TableInitializationException(e,tableDefinition);
            }
        }
    }
    default boolean isCurrentInitializer(TableDefinition tableDefinition, Map<String,Object> initParams){
        return (tableDefinition.getInitClass().equals(getClass().getName()));
    }

}
