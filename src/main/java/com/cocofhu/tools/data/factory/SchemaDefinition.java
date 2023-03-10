package com.cocofhu.tools.data.factory;

import com.cocofhu.tools.data.schema.SchemaInitializationException;
import com.cocofhu.tools.data.schema.SchemaInitializer;
import com.cocofhu.tools.data.schema.TableInitializationException;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;


@Getter
public class SchemaDefinition implements SchemaInitializer {
    private String name;
    private List<TableDefinition> tableDefinitions;
    private Map<String,String> attributes;
    private String initClass;


    @Override
    public Schema initCurrent(SchemaDefinition schemaDefinition, Map<String, Object> initParams) {
        throw new SchemaInitializationException(new UnsupportedOperationException("unsupported operation to initialize a schema. "), schemaDefinition);
    }
}
