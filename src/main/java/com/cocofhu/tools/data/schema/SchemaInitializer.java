package com.cocofhu.tools.data.schema;

import com.cocofhu.tools.data.schema.config.SchemaDefinition;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;


public interface SchemaInitializer {

    default Schema initCurrent(SchemaPlus root, SchemaDefinition definition, Context context){
        throw new SchemaInitializationException(new UnsupportedOperationException("unsupported operation to initialize a schema. "), definition);
    }

    default Schema initFully(SchemaPlus root, SchemaDefinition definition, Context context) {
        if (isCurrentInitializer(root, definition, context)) {
            return initCurrent(root, definition, context);
        } else {
            try {
                Class<?> clazz = Class.forName(definition.getInitClass());
                SchemaInitializer initializer = (SchemaInitializer) clazz.newInstance();
                return initializer.initFully(root, definition, context);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new SchemaInitializationException(e, definition);
            }
        }
    }

    default boolean isCurrentInitializer(SchemaPlus root, SchemaDefinition definition, Context context) {
        return (definition.getInitClass().equals(getClass().getName()));
    }
}
