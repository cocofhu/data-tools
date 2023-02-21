package com.cocofhu.tools.data.schema.file;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class AdvancedFileTable extends AbstractTable implements FilterableTable {

    private File file;

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {

        try {
            FileInputStream in = new FileInputStream(file);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }
}
