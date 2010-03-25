package me.arin.jacass;

import me.prettyprint.cassandra.dao.Command;
import org.apache.cassandra.thrift.ConsistencyLevel;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Arin Sarkissian
 * Date: Mar 17, 2010
 * Time: 5:33:17 PM
 */

/**
 * Help manage Cassandra connections and execute Hector methods
 * against Cassandra
 */
public class Executor {
    protected String keyspace;
    protected ArrayList<String> host = new ArrayList<String>();
    protected static ConcurrentHashMap<String, Executor> exectors = new ConcurrentHashMap<String, Executor>();
    protected ColumnCrud columnCrud;
    public static final String ALL_KEYSPACES = "*";

    protected Executor(String keyspace, String host, int port) {
        this.keyspace = keyspace;
        this.host.add(getHostString(host, port));
    }

    protected static String getHostString(String host, int port) {
        return new StringBuilder(host).append(":").append(port).toString();
    }

    public ColumnCrud getColumnCrud() {
        if (columnCrud == null) {
            columnCrud = new ColumnCrud(this);
        }

        return columnCrud;
    }

    public static Executor get() {
        return get(ALL_KEYSPACES);
    }

    public static Executor get(String name) {
        Executor executor = exectors.get(name);

        if (executor == null) {
            return exectors.get(ALL_KEYSPACES);
        }

        return executor;
    }

    public static Executor add(String host, int port) {
        return add(ALL_KEYSPACES, host, port);
    }

    public static Executor add(String keyspace, String host, int port) {
        Executor executor = exectors.get(keyspace);

        if (executor == null) {
            executor = new Executor(keyspace, host, port);
            exectors.put(keyspace, executor);
        }

        executor.host.add(getHostString(host, port));
        return executor;
    }

    protected <T> T execute(String keyspace, Command<T> command) throws Exception {
        return execute(keyspace, command, ConsistencyLevel.ONE);
    }

    protected <T> T execute(String keyspace, Command<T> command, ConsistencyLevel consistencyLevel) throws Exception {
        return command.execute(host.toArray(new String[0]), keyspace, consistencyLevel);
    }

    protected <T> T execute(BaseModel baseModel, Command<T> command) throws Exception {
        return execute(baseModel.getKeyspace(), command, ConsistencyLevel.ONE);
    }

    protected <T> T execute(BaseModel baseModel, Command<T> command, ConsistencyLevel consistencyLevel) throws Exception {
        return execute(baseModel.getKeyspace(), command, consistencyLevel);
    }
}
