package com.cocofhu.tools.data.schema.csv;

import com.cocofhu.tools.data.factory.FieldDefinition;
import com.cocofhu.tools.data.factory.TableDefinition;
import com.cocofhu.tools.data.schema.Context;
import com.cocofhu.tools.data.schema.MissingArgumentException;
import com.cocofhu.tools.data.schema.TableInitializationException;
import com.cocofhu.tools.data.schema.TableInitializer;
import com.cocofhu.utils.CollectionUtils;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SimpleCSVTableInitializer implements TableInitializer {

    public static final String LOCATION = "location";

    @Override
    public Table initCurrent(SchemaPlus root, TableDefinition definition, Context context) {
        // check arguments
        CollectionUtils.notExistKeys(definition.getAttributes(), new String[]{LOCATION}, key -> {
            throw new TableInitializationException(new MissingArgumentException(String.format("missing argument of attributes: %s. ", key)), definition);
        });
        String location = (String) definition.getAttributes().get(LOCATION);
        List<FieldDefinition> fields = definition.getFields();
        List<Pair<String, CSVFieldType>> types = new ArrayList<>();
        fields.forEach((fieldDefinition) -> {
            CSVFieldType type = CSVFieldType.of(fieldDefinition.getType());
            if (type == null) {
                throw new TableInitializationException(new IllegalArgumentException(String.format("illegal field type of csv table, %s .", fieldDefinition.getType())), definition);
            }
            types.add(new Pair<>(fieldDefinition.getName(), type));
        });
        return new CSVTable(new File(location), ((file, factory) -> types));
    }
}
