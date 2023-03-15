package com.cocofhu.tools.data.schema.mutable;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.InitializerContext;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import org.apache.calcite.schema.Schema;

public class MutableTableSchemaInitializer implements SchemaInitializer {
    @Override
    public Schema initCurrent(SchemaDefinition schemaDefinition, InitializerContext context) {
        MutableTableSchema schema = new MutableTableSchema();
        schemaDefinition.getTableDefinitions()
                .forEach(tableDefinition -> schema.putNewTable(tableDefinition.getName(),tableDefinition.initFully(tableDefinition, context)));
        return schema;
    }
}
