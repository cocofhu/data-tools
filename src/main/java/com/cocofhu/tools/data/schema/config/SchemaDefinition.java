package com.cocofhu.tools.data.schema.config;

import com.cocofhu.tools.data.schema.SchemaInitializer;
import lombok.Getter;

import java.util.List;
import java.util.Map;


@Getter
public class SchemaDefinition implements SchemaInitializer {
    private String name;
    private List<TableDefinition> tableDefinitions;
    private Map<String,Object> attributes;
    private String initClass;

}
