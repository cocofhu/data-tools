package com.cocofhu.tools.data.schema.file.csv;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.csv.CsvFilterableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

public class CSVTable extends AbstractTable implements FilterableTable {

    private final File file;

    private final RowTypeResolver rowTypeResolver;

    private RelDataType relDataType;

    public CSVTable(File file, RowTypeResolver rowTypeResolver) {
        this.file = file;
        this.rowTypeResolver = rowTypeResolver;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        this.getRowType(root.getTypeFactory());
        List<RelDataTypeField> fields = relDataType.getFieldList();
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                try {
                    return new CSVEnumerator(new BufferedReader(new FileReader(file)),fields);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if(this.relDataType == null){
            this.relDataType = rowTypeResolver.apply(this.file,typeFactory);
        }
        System.out.println(relDataType);
        return relDataType;
    }

    public interface RowTypeResolver extends Function2<File,RelDataTypeFactory,RelDataType>{
        static RowTypeResolver fromTypes(String[] names, SqlTypeName[] types){
            return (file, factory) -> factory.createStructType(Pair.zip(names,
                    Arrays.stream(types).map(factory::createSqlType)
                            .map(type->factory.createTypeWithNullability(type,true)).toArray(RelDataType[]::new)));
        }
    }
}
