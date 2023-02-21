package com.cocofhu.tools.data.schema.file;

import org.apache.calcite.linq4j.Enumerator;

import java.io.IOException;
import java.io.InputStream;

public class AdvancedFileEnumerator<E> implements Enumerator<E> {

    private final InputStream in;

    public AdvancedFileEnumerator(InputStream in) {
        this.in = in;
    }


    @Override
    public E current() {
        return null;
    }

    @Override
    public boolean moveNext() {
        return false;
    }

    @Override
    public void reset() {
        try {
            in.reset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            in.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
