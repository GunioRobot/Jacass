package me.arin.jacass;

import me.prettyprint.cassandra.dao.Command;

import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Arin Sarkissian
 * Date: Mar 17, 2010
 * Time: 5:33:17 PM
 */
public class Executor {
    protected String name;
    protected String keyspace;
    protected String host;
    protected int port;
    protected static ConcurrentHashMap<String, Executor> exectors = new ConcurrentHashMap<String, Executor>();

    public static final String DEFAULT_EXECUTOR_NAME = "__default__";

    protected Executor(String name, String keyspace, String host, int port) {
        this.name = name;
        this.keyspace = keyspace;
        this.host = host;
        this.port = port;
    }

    public static Executor get(String name) {
        return exectors.get(name);
    }

    public static Executor get() {
        return get(DEFAULT_EXECUTOR_NAME);
    }

    public static Executor add(String name, String keyspace, String host, int port) {
        Executor executor = exectors.get(keyspace);

        if (executor == null) {
            executor = new Executor(name, keyspace, host, port);
            exectors.put(name, executor);
        }

        return executor;
    }

    public static Executor add(String keyspace, String host, int port) {
        return add(DEFAULT_EXECUTOR_NAME, keyspace, host, port);
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    protected <T> T execute(Command<T> command) throws Exception {
        return command.execute(host, port, keyspace);
    }
}
