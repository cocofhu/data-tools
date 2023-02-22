package com.cocofhu.tools.data.schema.file.csv;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CSVEnumerator implements Enumerator<Object[]> {

    private final BufferedReader reader;
    private final Split split;
    private final AtomicBoolean cancelFlag;

    private final List<RelDataTypeField> fields;

    private Object[] current = new Object[0];



    public CSVEnumerator(BufferedReader reader,List<RelDataTypeField> fields, Split split, AtomicBoolean cancelFlag) {
        this.reader = reader;
        this.split = split;
        this.cancelFlag = cancelFlag;
        this.fields = fields;
    }
    public CSVEnumerator(BufferedReader reader,List<RelDataTypeField> fields, AtomicBoolean cancelFlag) {
        this(reader,fields, Split.COMMA,cancelFlag);
    }
    public CSVEnumerator(BufferedReader reader,List<RelDataTypeField> fields) {
        this(reader,fields, Split.COMMA,new AtomicBoolean(false));
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            if(cancelFlag.get()){
                return false;
            }
            String line = reader.readLine();
            if(line == null){
                return false;
            }
            current = this.split.split(line);
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        try {
            reader.reset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // CSV Split
    @FunctionalInterface
    public interface Split{

        static Split fromPattern(String pattern){
            return line -> line.split(pattern);
        }
        Split COMMA = fromPattern(",");
        Split SEMICOLON = fromPattern(",");
        Split WHITE_CHAR = fromPattern("\\s+");
        Split SPACE = fromPattern(" ");
        String[] split(String line);
    }
}
