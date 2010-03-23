package me.arin.jacass;

import me.arin.jacass.serializer.PrimitiveSerializer;
import me.prettyprint.cassandra.dao.Command;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;

import java.io.IOException;

/**
 * User: Arin Sarkissian
 * Date: Mar 10, 2010
 * Time: 4:56:38 PM
 */
public class ColumnCrud {
    protected PrimitiveSerializer serializer;
    protected Executor executor;

    public ColumnCrud(Executor executor) {
        serializer = new PrimitiveSerializer();
        this.executor = executor;
    }

    protected byte[] getRawBytes(final ColumnKey key) {
        Command<byte[]> command = new Command<byte[]>() {
            @Override
            public byte[] execute(Keyspace keyspace) throws Exception {
                Column column = keyspace.getColumn(key.getKey(), getColumnPath(key));

                if (column == null) {
                    return null;
                }

                return column.getValue();
            }
        };

        try {
            return executor.execute(command);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    protected ColumnPath getColumnPath(ColumnKey key) {
        ColumnPath columnKey = new ColumnPath(key.getColumnFamily());

        String superColumn = key.getSuperColumn();
        if (!"".equals(superColumn)) {
            columnKey.setSuper_column(superColumn.getBytes());
        }

        columnKey.setColumn(key.getColumnName().getBytes());
        return columnKey;
    }

    protected Object getColumnValue(ColumnKey key, Class cls) {
        try {
            return serializer.fromBytes(cls, getRawBytes(key));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public int getInt(ColumnKey key) {
        return getInt(key, 0);
    }

    public int getInt(ColumnKey key, int defaultValue) {
        Object value = getColumnValue(key, int.class);
        return (value != null) ? (Integer) value : defaultValue;
    }

    public float getFloat(ColumnKey key) {
        return getFloat(key, 0);
    }

    public float getFloat(ColumnKey key, float defaultValue) {
        Object value = getColumnValue(key, float.class);
        return (value != null) ? (Float) value : defaultValue;
    }

    public double getDouble(ColumnKey key) {
        return getDouble(key, 0);
    }

    public double getDouble(ColumnKey key, double defaultValue) {
        Object value = getColumnValue(key, double.class);
        return (value != null) ? (Double) value : defaultValue;
    }

    public boolean getBoolean(ColumnKey key) {
        return getBoolean(key, false);
    }

    public boolean getBoolean(ColumnKey key, boolean defaultValue) {
        Object value = getColumnValue(key, boolean.class);
        return (value != null) ? (Boolean) value : defaultValue;
    }

    public byte getByte(ColumnKey key) {
        return getByte(key, (byte) 0);
    }

    public byte getByte(ColumnKey key, byte defaultValue) {
        byte[] rawBytes = getRawBytes(key);
        return (rawBytes.length == 1) ? rawBytes[0] : defaultValue;
    }

    public char getChar(ColumnKey key) {
        return getChar(key, '\u0000');
    }

    public char getChar(ColumnKey key, char defaultValue) {
        Object value = getColumnValue(key, char.class);
        return (value != null) ? (Character) value : defaultValue;
    }

    public long getLong(ColumnKey key) {
        return getLong(key, 0);
    }

    public long getLong(ColumnKey key, long defaultValue) {
        Object value = getColumnValue(key, long.class);
        return (value != null) ? (Long) value : defaultValue;
    }

    public short getShort(ColumnKey key) {
        return getShort(key, (short) 0);
    }

    public short getShort(ColumnKey key, short defaultValue) {
        Object value = getColumnValue(key, short.class);
        return (value != null) ? (Short) value : defaultValue;
    }

    public String getString(ColumnKey key) {
        return getString(key, null);
    }

    public String getString(ColumnKey key, String defaultValue) {
        byte[] rawBytes = getRawBytes(key);
        return (rawBytes.length != 0) ? new String(rawBytes) : defaultValue;
    }

    public byte[] getRaw(ColumnKey key) {
        return getRawBytes(key);
    }

    public void set(final ColumnKey key, final Object value) {
        final ColumnPath columnPath = getColumnPath(key);

        Command<Void> command = new Command<Void>() {
            @Override
            public Void execute(Keyspace keyspace) throws Exception {
                keyspace.insert(key.getKey(), columnPath, serializer.toBytes(value.getClass(), value));
                return null;
            }
        };

        try {
            executor.execute(command);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void remove(final ColumnKey key) {
        Command<Void> command = new Command<Void>() {
            @Override
            public Void execute(Keyspace keyspace) throws Exception {
                keyspace.remove(key.getKey(), getColumnPath(key));
                return null;
            }
        };

        try {
            executor.execute(command);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean exists(ColumnKey key) {
        return getRawBytes(key).length > 1;
    }
}