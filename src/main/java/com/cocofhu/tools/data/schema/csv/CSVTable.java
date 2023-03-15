package com.cocofhu.tools.data.schema.csv;

import com.cocofhu.tools.data.factory.FieldDefinition;
import com.cocofhu.tools.data.factory.TableDefinition;
import com.cocofhu.tools.data.schema.InitializerContext;
import com.cocofhu.tools.data.schema.MissingArgumentException;
import com.cocofhu.tools.data.schema.TableInitializationException;
import com.cocofhu.tools.data.schema.TableInitializer;
import com.cocofhu.utils.CollectionUtils;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CSVTable extends AbstractTable implements FilterableTable {

    private final File file;

    private final RowTypeResolver rowTypeResolver;

    private RelDataType relDataType;

    private CSVFieldType[] fieldTypes;

    public CSVTable(File file, RowTypeResolver rowTypeResolver) {
        this.file = file;
        this.rowTypeResolver = rowTypeResolver;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        this.getRowType(root.getTypeFactory());
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                try {
                    return new CSVEnumerator(new BufferedReader(new FileReader(file)),fieldTypes);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if(this.relDataType == null){
            this.relDataType = rowTypeResolver.resolveRelDataTypes(this.file,typeFactory);
            this.fieldTypes = rowTypeResolver.resolveFields(this.file,typeFactory).stream().map(e->e.right).toArray(CSVFieldType[]::new);
        }
        return relDataType;
    }

    /**
     *  表字段获取器：获取表的列名与列类型
     */
    @FunctionalInterface
    public interface RowTypeResolver{

        List<Pair<String,CSVFieldType>> resolveFields(File file, RelDataTypeFactory factory);
        default RelDataType resolveRelDataTypes(File file, RelDataTypeFactory factory){
            return factory.createStructType(resolveFields(file, factory).stream().map(pair -> new Pair<>(pair.left, Objects.requireNonNull(pair.right).toType(factory))).collect(Collectors.toList()));
        }

    }
}
