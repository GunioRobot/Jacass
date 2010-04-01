package me.arin.jacass.serializer;

import me.arin.jacass.JacassException;
import me.arin.jacass.Serializer;

import java.io.IOException;

/**
 * User: Arin Sarkissian
 * Date: Mar 31, 2010
 * Time: 9:39:08 PM
 */

/**
 * This serializer forces everything to be stored in Cassandra as a String
 */
public class StringSerializer implements Serializer {
    public byte[] toBytes(Object value) throws JacassException {
        return value.toString().getBytes();
    }

    public byte[] toBytes(Class cls, Object value) throws JacassException {
        return value.toString().getBytes();
    }

    public String fromBytes(byte[] bytes) throws IOException {
        return new String(bytes);
    }

    public String fromBytes(Class cls, byte[] bytes) throws IOException {
        return new String(bytes);
    }
}
