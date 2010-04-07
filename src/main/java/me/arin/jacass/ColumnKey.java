package me.arin.jacass;

/**
 * User: Arin Sarkissian
 * Date: Mar 10, 2010
 * Time: 7:03:19 PM
 */
public class ColumnKey {
    private String keyspace = "";
    private String columnFamily = "";
    private String superColumn = "";
    private String key = "";

    // TODO: should this be a byte[]
    private String columnName;

    public ColumnKey(String keyspace, String superColumn, String columnFamily, String key, String columnName) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.superColumn = superColumn;
        this.key = key;
        this.columnName = columnName;
    }

    public ColumnKey(String keyspace, String columnFamily, String key, String columnName) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.key = key;
        this.columnName = columnName;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getSuperColumn() {
        return superColumn;
    }

    public void setSuperColumn(String superColumn) {
        this.superColumn = superColumn;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
