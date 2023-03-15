package com.cocofhu.tools.data.schema;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import org.apache.calcite.schema.Schema;

import java.util.Map;


public interface SchemaInitializer {

    Schema initCurrent(SchemaDefinition schemaDefinition, InitializerContext context);
    default Schema initFully(SchemaDefinition schemaDefinition, InitializerContext context){
        if(isCurrentInitializer(schemaDefinition, context)){
            return initCurrent(schemaDefinition, context);
        }else{
            try {
                Class<?> clazz = Class.forName(schemaDefinition.getInitClass());
                SchemaInitializer initializer = (SchemaInitializer) clazz.newInstance();
                return initializer.initFully(schemaDefinition, context);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new SchemaInitializationException(e,schemaDefinition);
            }
        }
    }
    default boolean isCurrentInitializer(SchemaDefinition schemaDefinition, InitializerContext context){
        return (schemaDefinition.getInitClass().equals(getClass().getName()));
    }
}
