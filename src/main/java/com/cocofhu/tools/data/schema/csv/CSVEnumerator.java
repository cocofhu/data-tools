package com.cocofhu.tools.data.schema.csv;

import org.apache.calcite.linq4j.Enumerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CSVEnumerator implements Enumerator<Object[]> {

    private final BufferedReader reader;
    private final Split split;
    private final AtomicBoolean cancelFlag;

    private final CSVFieldType[] fields;

    private final Object[] current;


    public CSVEnumerator(BufferedReader reader,CSVFieldType[] fields, Split split, AtomicBoolean cancelFlag) {
        this.reader = reader;
        this.split = split;
        this.cancelFlag = cancelFlag;
        this.fields = fields;
        this.current = new Object[fields.length];
    }
    public CSVEnumerator(BufferedReader reader,CSVFieldType[] fields, AtomicBoolean cancelFlag) {
        this(reader,fields, Split.COMMA,cancelFlag);
    }
    public CSVEnumerator(BufferedReader reader,CSVFieldType[] fields) {
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
            // 按照定义的列来处理，如果fields长度不足后面的数据将会忽略，如果数据不足则使用空值
            String[] row = this.split.split(line, fields.length);
            for (int i = 0; i < fields.length; i++) {
                if(i < row.length) current[i] = fields[i].convert(row[i]);
                else current[i] = null;
            }
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
            return (line, expectationSize) -> line.split(pattern, expectationSize +1);
        }
        Split COMMA = fromPattern(",");
        Split SEMICOLON = fromPattern(",");
        // 不推荐使用，可用使用更好的正则算法进行优化
        @Deprecated
        Split WHITE_CHAR = fromPattern("\\s+");
        Split SPACE = fromPattern(" ");

        /**
         * 拆分CSV的每一行，如果返回列数少于期望的列数，将会用null补充，
         * 如果超出期望的列数多余的列数，多余的列将会被忽略，应该尽量保证
         * 返回的列数与期望列数一致，以获得更好的性能
         */
        String[] split(String line,int expectationSize);
    }
}
