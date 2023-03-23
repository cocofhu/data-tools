package com.cocofhu.tools.data.schema;


import com.cocofhu.tools.data.factory.TableDefinition;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;


public interface TableInitializer {
    Table initCurrent(SchemaPlus root, TableDefinition definition, Context context);

    default Table initFully(SchemaPlus root, TableDefinition definition, Context context) {
        if (isCurrentInitializer(root, definition, context)) {
            return initCurrent(root, definition, context);
        } else {
            try {
                Class<?> clazz = Class.forName(definition.getInitClass());
                TableInitializer initializer = (TableInitializer) clazz.newInstance();
                return initializer.initFully(root, definition, context);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new TableInitializationException(e, definition);
            }
        }
    }

    default boolean isCurrentInitializer(SchemaPlus root, TableDefinition definition, Context context) {
        return (definition.getInitClass().equals(getClass().getName()));
    }

}
