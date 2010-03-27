package me.arin.jacass;

/**
 * Info about a Column
 */
class ColumnInfo {
    private String name;
    private Class cls;
    private IndexInfo indexData;

    ColumnInfo(String name, Class cls) {
        this.name = name;
        this.cls = cls;
    }

    public void setIndexData(IndexInfo indexData) {
        this.indexData = indexData;
    }

    public boolean isIndexed() {
        return (indexData != null);
    }

    public String getName() {
        return name;
    }

    public Class getCls() {
        return cls;
    }

    public IndexInfo getIndexData() {
        return indexData;
    }
}
