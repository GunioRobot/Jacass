package me.arin.jacass;

import java.lang.reflect.Field;

/**
 * Info about a Column
 */
class ColumnInfo {
    private final Field field;
    private IndexInfo indexData;

    ColumnInfo(Field field) {
        this.field = field;
        this.field.setAccessible(true);
    }

    public void setIndexData(IndexInfo indexData) {
        this.indexData = indexData;
    }

    public boolean isIndexed() {
        return (indexData != null);
    }

    public String getName() {
        return field.getName();
    }

    public Class getCls() {
        return field.getType();
    }

    public Field getField() {
        return field;
    }

    public IndexInfo getIndexData() {
        return indexData;
    }
}
