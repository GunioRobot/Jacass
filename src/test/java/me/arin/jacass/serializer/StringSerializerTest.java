package me.arin.jacass.serializer;

import junit.framework.TestCase;
import me.arin.jacass.Serializer;

import java.util.Arrays;

/**
 * User: Arin Sarkissian
 * Date: Mar 31, 2010
 * Time: 9:42:55 PM
 */
public class StringSerializerTest extends TestCase {
    Serializer ser = new StringSerializer();

    public void testToBytes() throws Exception {
        int i = 666;
        String s = "666";

        assertTrue(Arrays.equals(s.getBytes(), ser.toBytes(i)));
        assertTrue(Arrays.equals(s.getBytes(), ser.toBytes(String.class, i)));
    }

    public void testFromBytes() throws Exception {
        byte[] bytes = ser.toBytes(666);

        assertEquals("666", ser.fromBytes(bytes));
        assertEquals("666", ser.fromBytes(String.class, bytes));
    }
}
