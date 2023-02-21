package com.cocofhu.tools.data.schema.file.csv;

import org.apache.calcite.linq4j.Enumerator;

import java.io.BufferedReader;
import java.nio.Buffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class CSVEnumerator implements Enumerator<Object[]> {




    private BufferedReader reader;
    private Split split;
    private AtomicBoolean cancelFlag;
    private Object[] current = new Object[0];

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public void close() {

    }

    // CSV Split
    @FunctionalInterface
    public interface Split{

        static Split fromPattern(String pattern){
            return line -> line.split(pattern);
        }
        Split COMMA = fromPattern(",");
        Split WHITE_CHAR = fromPattern("\\s+");
        Split SPACE = fromPattern(" ");
        String[] split(String line);
    }
}
