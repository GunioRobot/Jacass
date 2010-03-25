package me.arin.jacass;

import java.io.IOException;

/**
 * User: Arin Sarkissian
 * Date: Mar 23, 2010
 * Time: 11:44:58 AM
 */

/**
 * Interface for (de)serializing Column values to/from Cassandra
 */
public interface Serializer {
    byte[] toBytes(Object value) throws JacassException;

    byte[] toBytes(Class cls, Object value) throws JacassException;

    Object fromBytes(byte[] bytes) throws IOException;

    Object fromBytes(Class cls, byte[] bytes) throws IOException;
}
