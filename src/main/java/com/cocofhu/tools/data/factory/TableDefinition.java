package com.cocofhu.tools.data.factory;

import com.cocofhu.tools.data.schema.Context;
import com.cocofhu.tools.data.schema.TableInitializationException;
import com.cocofhu.tools.data.schema.TableInitializer;
import lombok.Getter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import java.util.List;
import java.util.Map;


@Getter
public class TableDefinition implements TableInitializer {
    private String name;
    private List<FieldDefinition> fields;
    private Map<String,Object> attributes;
    private String initClass;

    @Override
    public Table initCurrent(SchemaPlus root, TableDefinition definition, Context context) {
        throw new TableInitializationException(new UnsupportedOperationException("unsupported operation to initialize a table. "), definition);
    }
}
