package me.arin.jacass;

/**
 * User: Arin Sarkissian
 * Date: Apr 7, 2010
 * Time: 11:59:20 AM
 */
public class ColumnKeyValue {
    private ColumnKey columnKey;
    private byte[] columnValue;

    public ColumnKeyValue(ColumnKey columnKey, byte[] columnValue) {
        this.columnKey = columnKey;
        this.columnValue = columnValue;
    }

    public ColumnKey getColumnKey() {
        return columnKey;
    }

    public void setColumnKey(ColumnKey columnKey) {
        this.columnKey = columnKey;
    }

    public byte[] getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(byte[] columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public String toString() {
        return columnKey.toString() + ":" + new String(columnValue);
    }
}
