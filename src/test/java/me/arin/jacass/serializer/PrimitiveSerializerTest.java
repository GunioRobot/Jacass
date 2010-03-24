package me.arin.jacass.serializer;

import junit.framework.TestCase;

/**
 * User: Arin Sarkissian
 * Date: Mar 23, 2010
 * Time: 1:06:44 PM
 */
public class PrimitiveSerializerTest extends TestCase {
    public boolean isSameBytes(byte[] a, byte[] b) {
        if (a == null && b == null) {
            return true;
        } else if (a == null || b == null) {
            return false;
        } else if (a.length != a.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }

        return true;
    }

    public void testToBytes() throws Exception {
        PrimitiveSerializer p = new PrimitiveSerializer();
        assertTrue(isSameBytes("hello".getBytes(), p.toBytes("hello")));
    }

    public void testFromBytes() throws Exception {
        String hello = "hello";
        byte[] bytes = hello.getBytes();
        PrimitiveSerializer p = new PrimitiveSerializer();

        assertEquals(hello, p.fromBytes(bytes));
    }
}