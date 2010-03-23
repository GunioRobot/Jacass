package me.arin.jacass;

/**
 * User: Arin Sarkissian
 * Date: Mar 10, 2010
 * Time: 2:06:10 PM
 */
public class RowPath {
    protected String keyspace;
    protected String columnFamily;
    protected String superColumn;

    public RowPath(String keyspace, String columnFamily, String superColumn) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.superColumn = superColumn;
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
}
