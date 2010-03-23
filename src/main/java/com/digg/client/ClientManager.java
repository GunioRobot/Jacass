package com.digg.client;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;

/**
 * User: Arin Sarkissian
 * Date: Mar 3, 2010
 * Time: 5:40:34 PM
 */
public class ClientManager {
    protected Class clientClass;
    protected ThriftClientPool thriftClientPool;
    protected int numRetries = 3;
    protected boolean isAutoRelease = true;
    protected boolean isSetup = false;
    protected HashMap<String, HostPort> hosts = new HashMap<String, HostPort>();
    protected static HashMap<Class, ClientManager> managers = new HashMap<Class, ClientManager>();

    public static ClientManager factory(Class clientClass) {
        ClientManager clientManager = managers.get(clientClass);
        if (clientManager == null) {
            clientManager = new ClientManager(clientClass);
            managers.put(clientClass, clientManager);
        }

        return clientManager;
    }

    public ClientManager(Class clientClass) {
        this.clientClass = clientClass;
    }

    public void addHost(String host, int port) {
        hosts.put(host + ":" + port, new HostPort(host, port));
    }

    protected void setupPool() {
        setPool(new ThriftClientPool(new ThriftClientPoolFactory(clientClass, hosts.values())));
    }

    public Object getClient() {
        if (!isSetup) {
            setupPool();
        }

        try {
            Object client = thriftClientPool.borrowObject();
            return client;
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public int getNumRetries() {
        return numRetries;
    }

    public void setNumRetries(int numRetries) {
        this.numRetries = numRetries;
    }

    public boolean isAutoRelease() {
        return isAutoRelease;
    }

    public void setAutoRelease(boolean autoRelease) {
        isAutoRelease = autoRelease;
    }

    protected void setPool(ThriftClientPool thriftClientPool) {
        this.thriftClientPool = thriftClientPool;
    }

    public ThriftClientPool getThriftClientPool() {
        return thriftClientPool;
    }

    public void release(Object client) {
        try {
            thriftClientPool.returnObject(client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ClientOperation execute(ClientOperation operation, Object client) {
        try {
            operation.returnValue = operation.execute();
        } catch (TApplicationException e) {
            // TODO: your endpoint is broken
        } catch (TException e) {
            // TODO: ???
            e.printStackTrace();
        } catch (Exception e) {
            // TODO: ???
        }

        if (isAutoRelease) {
            release(client);
        }

        return operation;
    }

    public Object getResult(ClientOperation operation, Object client) {
        ClientOperation clientOperation = execute(operation, client);
        return clientOperation.getReturnValue();
    }

}