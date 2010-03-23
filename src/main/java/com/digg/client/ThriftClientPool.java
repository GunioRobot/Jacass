package com.digg.client;

import com.digg.common.thrift.ClientDoeNotExistException;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;

/**
 * User: Arin Sarkissian
 * Date: Feb 26, 2010
 * Time: 3:28:41 PM
 */
public class ThriftClientPool extends GenericObjectPool {
    public static final boolean DEFAULT_TEST_ON_BORROW = false;

    public ThriftClientPool(PoolableObjectFactory objFactory) {
        super(objFactory);
        setupClientOptions();
    }

    protected void setupClientOptions() {
        this.setMaxIdle(DEFAULT_MAX_IDLE);
        this.setMaxActive(DEFAULT_MAX_ACTIVE);
        this.setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        this.setTestOnBorrow(DEFAULT_TEST_ON_BORROW);
        this.setMaxWait(DEFAULT_MAX_WAIT);
    }

    public Object borrowObject() throws Exception {
        return super.borrowObject();
    }

    public void returnObject(Object obj) throws Exception {
        super.returnObject(obj);
    }
}

class ThriftClientPoolFactory implements PoolableObjectFactory {
    protected Class cls;
    protected Collection<HostPort> hostPorts;

    ThriftClientPoolFactory(Class cls, Collection<HostPort> hostPorts) {
        this.cls = cls;
        this.hostPorts = hostPorts;
    }

    protected Object createClient(Class cls, String host, int port) throws ClientDoeNotExistException {
        try {
            Class params[] = {TProtocol.class};
            TTransport transport = new TSocket(host, port);
            Constructor constructor = cls.getConstructor(params);

            return constructor.newInstance(new TBinaryProtocol(transport));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public Object makeObject() throws Exception {
        if (hostPorts.isEmpty()) {
            throw new Exception("No host info yo");
        }

        // TODO: round robin thru the hosts
        HostPort hostPort = hostPorts.iterator().next();

        Object client = createClient(cls, hostPort.getHost(), hostPort.getPort());
        Method getInputProtocol = cls.getMethod("getInputProtocol");
        getInputProtocol.invoke(client);

        TProtocol inputProtocol = (TProtocol) getInputProtocol.invoke(client);
        inputProtocol.getTransport().open();

        return client;
    }

    public void destroyObject(Object o) throws Exception {

    }

    public boolean validateObject(Object o) {
        return true;
    }

    public void activateObject(Object o) throws Exception {

    }

    public void passivateObject(Object client) throws Exception {
        Method getInputProtocol = cls.getMethod("getInputProtocol");
        getInputProtocol.invoke(client);
        TProtocol inputProtocol = (TProtocol) getInputProtocol.invoke(client);
        inputProtocol.getTransport().close();
    }
}