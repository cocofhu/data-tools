package com.cocofhu.tools.data.schema.mutable;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.Context;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

public class MutableTableSchemaInitializer implements SchemaInitializer {

    @Override
    public Schema initCurrent(SchemaPlus root, SchemaDefinition definition, Context context) {
        MutableTableSchema schema = new MutableTableSchema();
        definition.getTableDefinitions()
                .forEach(tableDefinition -> schema.putNewTable(tableDefinition.getName(),tableDefinition.initFully(root, tableDefinition, context)));
        return schema;
    }
}
