package com.cocofhu.tools.data.schema;

import com.cocofhu.tools.data.schema.config.TableDefinition;
import lombok.Getter;

public class TableInitializationException extends RuntimeException {
    @Getter
    private final Throwable target;
    @Getter
    private final TableDefinition tableDefinition;

    public TableInitializationException(Throwable target, TableDefinition tableDefinition) {
        super(String.format("initialize table failed, message: %s. ", target.getMessage()));
        this.target = target;
        this.tableDefinition = tableDefinition;
    }
}
