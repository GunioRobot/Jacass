package me.arin.jacass;

import com.digg.client.ClientManager;
import org.apache.cassandra.thrift.Cassandra;

/**
 * User: Arin Sarkissian
 * Date: Mar 15, 2010
 * Time: 4:14:08 PM
 */
public class ColumnFamily {
    private final String keyspace;
    private final String columnFamily;

    public ColumnFamily(String keyspace, String columnFamily) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    protected ClientManager getClientManager() {
        return ClientManager.factory(Cassandra.Client.class);
    }

    public void insert(String key, String columnName, Object columnValue) {
        ColumnCrud cc = new ColumnCrud();
    }

    public void insert(String key, String superColumn, String columnName, Object columnValue) {
        insert(key, superColumn, columnName.getBytes(), columnValue);
    }

    public void insert(String key, String superColumn, byte[] columnName, Object columnValue) {

    }
}
