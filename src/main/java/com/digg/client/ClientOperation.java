package com.digg.client;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

/**
 * User: Arin Sarkissian
 * Date: Mar 12, 2010
 * Time: 8:26:05 PM
 */
public abstract class ClientOperation<T> {
    protected T returnValue;

    public abstract T execute() throws TException, TimedOutException, NotFoundException, InvalidRequestException,
            UnavailableException;

    public T getReturnValue() {
        return returnValue;
    }
}
