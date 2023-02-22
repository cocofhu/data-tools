package com.cocofhu.tools.data.convert;

import org.apache.calcite.rel.type.RelDataType;

/**
 *
 */
public interface SqlTypeConverter<T> {
    T convert(T raw,RelDataType type);
}
