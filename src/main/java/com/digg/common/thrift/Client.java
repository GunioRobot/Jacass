package com.digg.common.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * User: arin
 * Date: Feb 26, 2010
 * Time: 11:42:47 AM
 */
public class Client {

    public static Object factory(Class clientClass, String host, int port) throws ClientDoeNotExistException {
        try {
            Class params[] = {TProtocol.class};
            TTransport transport = new TSocket(host, port);
            Constructor constructor = clientClass.getConstructor(params);

            return constructor.newInstance(new TBinaryProtocol(transport));
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return null;
    }
}