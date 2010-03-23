package me.arin.jacass;

import com.digg.client.ClientManager;
import com.digg.client.ClientOperation;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;

import java.io.IOException;

/**
 * User: Arin Sarkissian
 * Date: Mar 10, 2010
 * Time: 4:56:38 PM
 */
public class ColumnCrud {
    protected static byte[] getRawBytes(final ColumnKey key) {
        final ColumnPath columnKey = getColumnPath(key);
        final ClientManager clientManager = getCassandraClientManager();

        ClientOperation<byte[]> operation = new ClientOperation<byte[]>() {
            public byte[] execute() throws TException, TimedOutException, NotFoundException, InvalidRequestException,
                    UnavailableException {
                Cassandra.Client client = (Cassandra.Client) clientManager.getClient();
                ColumnOrSuperColumn column = client
                        .get(key.getKeyspace(), key.getKey(), columnKey, ConsistencyLevel.ONE);

                return column.getColumn().getValue();
            }
        };

        return (byte[]) clientManager.getResult(operation, clientManager);
    }

    protected static ColumnPath getColumnPath(ColumnKey key) {
        ColumnPath columnKey = new ColumnPath(key.getColumnFamily());

        String superColumn = key.getSuperColumn();
        if (!"".equals(superColumn)) {
            columnKey.setSuper_column(superColumn.getBytes());
        }

        columnKey.setColumn(key.getColumnName().getBytes());
        return columnKey;
    }

    protected static Object getColumnValue(ColumnKey key, Class cls) {
        try {
            return Caster.fromBytes(cls, getRawBytes(key));
        } catch (IOException e) {
            return null;
        }
    }

    protected static ClientManager getCassandraClientManager() {
        return ClientManager.factory(Cassandra.Client.class);
    }

    public static int getInt(ColumnKey key) {
        return getInt(key, 0);
    }

    public static int getInt(ColumnKey key, int defaultValue) {
        Object value = getColumnValue(key, int.class);
        return (value != null) ? (Integer) value : defaultValue;
    }

    public static float getFloat(ColumnKey key) {
        return getFloat(key, 0);
    }

    public static float getFloat(ColumnKey key, float defaultValue) {
        Object value = getColumnValue(key, float.class);
        return (value != null) ? (Float) value : defaultValue;
    }

    public static double getDouble(ColumnKey key) {
        return getDouble(key, 0);
    }

    public static double getDouble(ColumnKey key, double defaultValue) {
        Object value = getColumnValue(key, double.class);
        return (value != null) ? (Double) value : defaultValue;
    }

    public static boolean getBoolean(ColumnKey key) {
        return getBoolean(key, false);
    }

    public static boolean getBoolean(ColumnKey key, boolean defaultValue) {
        Object value = getColumnValue(key, boolean.class);
        return (value != null) ? (Boolean) value : defaultValue;
    }

    public static byte getByte(ColumnKey key) {
        return getByte(key, (byte) 0);
    }

    public static byte getByte(ColumnKey key, byte defaultValue) {
        byte[] rawBytes = getRawBytes(key);
        return (rawBytes.length == 1) ? rawBytes[0] : defaultValue;
    }

    public static char getChar(ColumnKey key) {
        return getChar(key, '\u0000');
    }

    public static char getChar(ColumnKey key, char defaultValue) {
        Object value = getColumnValue(key, char.class);
        return (value != null) ? (Character) value : defaultValue;
    }

    public static long getLong(ColumnKey key) {
        return getLong(key, 0);
    }

    public static long getLong(ColumnKey key, long defaultValue) {
        Object value = getColumnValue(key, long.class);
        return (value != null) ? (Long) value : defaultValue;
    }

    public static short getShort(ColumnKey key) {
        return getShort(key, (short) 0);
    }

    public static short getShort(ColumnKey key, short defaultValue) {
        Object value = getColumnValue(key, short.class);
        return (value != null) ? (Short) value : defaultValue;
    }

    public static String getString(ColumnKey key) {
        return getString(key, null);
    }

    public static String getString(ColumnKey key, String defaultValue) {
        byte[] rawBytes = getRawBytes(key);
        return (rawBytes.length != 0) ? new String(rawBytes) : defaultValue;
    }

    public static byte[] getRaw(ColumnKey key) {
        return getRawBytes(key);
    }

    public static void set(final ColumnKey key, Object value) {
        ClientManager clientManager = getCassandraClientManager();
        final Cassandra.Client client = (Cassandra.Client) clientManager.getClient();
        final ColumnPath columnKey = getColumnPath(key);
        final byte[] bytes = Caster.toBytes(value.getClass(), value);

        ClientOperation<Void> clientOperation = new ClientOperation<Void>() {
            @Override
            public Void execute() throws TException, TimedOutException, NotFoundException, InvalidRequestException,
                    UnavailableException {
                client.insert(key.getKeyspace(), key.getKey(), columnKey, bytes, System.currentTimeMillis(),
                              ConsistencyLevel.ONE);
                return null;
            }
        };

        clientManager.execute(clientOperation, client);
    }

    public static void remove(final ColumnKey key) {
        final ColumnPath cp = getColumnPath(key);
        final ClientManager clientManager = getCassandraClientManager();
        final Cassandra.Client client = (Cassandra.Client) clientManager.getClient();

        ClientOperation<Void> operation = new ClientOperation<Void>() {
            @Override
            public Void execute() throws TException, TimedOutException, NotFoundException, InvalidRequestException,
                    UnavailableException {

                client.remove(key.getKeyspace(), key.getKey(), cp, System.currentTimeMillis(), ConsistencyLevel.ONE);
                return null;
            }
        };

        clientManager.execute(operation, client);
    }

    public static boolean exists(ColumnKey key) {
        return getRawBytes(key).length > 1;
    }
}