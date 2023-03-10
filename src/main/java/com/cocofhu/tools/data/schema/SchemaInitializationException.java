package com.cocofhu.tools.data.schema;

import com.cocofhu.tools.data.factory.SchemaDefinition;
import com.cocofhu.tools.data.factory.TableDefinition;
import lombok.Getter;

public class SchemaInitializationException extends RuntimeException {
    @Getter
    private final Throwable target;
    @Getter
    private final SchemaDefinition schemaDefinition;

    public SchemaInitializationException(Throwable target, SchemaDefinition schemaDefinition) {
        super(String.format("initialize schema failed, message: %s. ", target.getMessage()));
        this.target = target;
        this.schemaDefinition = schemaDefinition;
    }
}
