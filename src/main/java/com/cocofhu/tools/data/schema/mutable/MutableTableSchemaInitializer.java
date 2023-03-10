package com.cocofhu.tools.data.schema.mutable;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import org.apache.calcite.schema.Schema;

import java.util.Map;

public class MutableTableSchemaInitializer implements SchemaInitializer {

    @Override
    public Schema initCurrent(SchemaDefinition schemaDefinition, Map<String, Object> initParams) {
        MutableTableSchema schema = new MutableTableSchema();
        schemaDefinition.getTableDefinitions()
                .forEach(tableDefinition -> schema.putNewTable(tableDefinition.getName(),tableDefinition.initFully(tableDefinition, initParams)));
        return schema;
    }
}
