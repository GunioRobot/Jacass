package me.arin.jacass;

import java.io.IOException;

/**
 * User: Arin Sarkissian
 * Date: Mar 23, 2010
 * Time: 11:44:58 AM
 */
public interface Serializer {
    byte[] toBytes(Object value);

    Object fromBytes(byte[] bytes) throws IOException;

    Object fromBytes(Class cls, byte[] bytes) throws IOException;
}
