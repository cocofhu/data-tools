package com.cocofhu.tools.data.schema;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import org.apache.calcite.schema.Schema;

import java.util.Map;


public interface SchemaInitializer {

    // SchemaPlus for jdbc initialization
    String PARENT_SCHEMA = "PARENT_SCHEMA";

    Schema initCurrent(SchemaDefinition schemaDefinition, Map<String,Object> initParams);
    default Schema initFully(SchemaDefinition schemaDefinition, Map<String,Object> initParams){
        if(isCurrentInitializer(schemaDefinition, initParams)){
            return initCurrent(schemaDefinition, initParams);
        }else{
            try {
                Class<?> clazz = Class.forName(schemaDefinition.getInitClass());
                SchemaInitializer initializer = (SchemaInitializer) clazz.newInstance();
                return initializer.initFully(schemaDefinition, initParams);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new SchemaInitializationException(e,schemaDefinition);
            }
        }
    }
    default boolean isCurrentInitializer(SchemaDefinition schemaDefinition, Map<String,Object> initParams){
        return (schemaDefinition.getInitClass().equals(getClass().getName()));
    }
}
