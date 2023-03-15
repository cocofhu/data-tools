package com.cocofhu.tools.data.schema;


import com.cocofhu.tools.data.factory.TableDefinition;
import org.apache.calcite.schema.Table;

import java.util.Map;


public interface TableInitializer {
    Table initCurrent(TableDefinition tableDefinition, InitializerContext context);
    default Table initFully(TableDefinition tableDefinition, InitializerContext context){
        if(isCurrentInitializer(tableDefinition, context)){
            return initCurrent(tableDefinition,context);
        }else{
            try {
                Class<?> clazz = Class.forName(tableDefinition.getInitClass());
                TableInitializer initializer = (TableInitializer) clazz.newInstance();
                return initializer.initFully(tableDefinition,context);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new TableInitializationException(e,tableDefinition);
            }
        }
    }
    default boolean isCurrentInitializer(TableDefinition tableDefinition, InitializerContext context){
        return (tableDefinition.getInitClass().equals(getClass().getName()));
    }

}
