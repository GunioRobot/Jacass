package me.arin.jacass.serializer;

import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

/**
 * User: Arin Sarkissian
 * Date: Mar 23, 2010
 * Time: 1:06:44 PM
 */
public class PrimitiveSerializerTest extends TestCase {
    public void testToBytesWithType() throws Exception {
        String theString = "theString";
        int theInt = Integer.MAX_VALUE;
        byte theByte = Byte.MAX_VALUE;
        short theShort = Short.MAX_VALUE;
        long theLong = Long.MAX_VALUE;
        float theFloat = Float.MAX_VALUE;
        double theDouble = Double.MAX_VALUE;
        char theChar = 'a';
        boolean theBoolean = Boolean.TRUE;

        PrimitiveSerializer p = new PrimitiveSerializer();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        assertTrue(Arrays.equals(theString.getBytes(), p.toBytes(String.class, theString)));

        dout.writeInt(theInt);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(int.class, theInt)));
        bout.reset();

        dout.writeByte(theByte);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(byte.class, theByte)));
        bout.reset();

        dout.writeShort(theShort);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(short.class, theShort)));
        bout.reset();

        dout.writeLong(theLong);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(long.class, theLong)));
        bout.reset();

        dout.writeFloat(theFloat);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(float.class, theFloat)));
        bout.reset();

        dout.writeDouble(theDouble);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(double.class, theDouble)));
        bout.reset();

        dout.writeChar(theChar);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(char.class, theChar)));
        bout.reset();

        dout.writeBoolean(theBoolean);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(boolean.class, theBoolean)));
        bout.reset();
    }

    public void testToBytesWithoutType() throws Exception {
        String theString = "theString";
        int theInt = Integer.MAX_VALUE;
        byte theByte = Byte.MAX_VALUE;
        short theShort = Short.MAX_VALUE;
        long theLong = Long.MAX_VALUE;
        float theFloat = Float.MAX_VALUE;
        double theDouble = Double.MAX_VALUE;
        char theChar = 'a';
        boolean theBoolean = Boolean.TRUE;

        PrimitiveSerializer p = new PrimitiveSerializer();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        assertTrue(Arrays.equals(theString.getBytes(), p.toBytes(theString)));

        dout.writeInt(theInt);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theInt)));
        bout.reset();

        dout.writeByte(theByte);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theByte)));
        bout.reset();

        dout.writeShort(theShort);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theShort)));
        bout.reset();

        dout.writeLong(theLong);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theLong)));
        bout.reset();

        dout.writeFloat(theFloat);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theFloat)));
        bout.reset();

        dout.writeDouble(theDouble);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theDouble)));
        bout.reset();

        dout.writeChar(theChar);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theChar)));
        bout.reset();

        dout.writeBoolean(theBoolean);
        assertTrue(Arrays.equals(bout.toByteArray(), p.toBytes(theBoolean)));
        bout.reset();
    }

    public void testFromBytes() throws Exception {
        String theString = "theString";
        int theInt = Integer.MAX_VALUE;
        byte theByte = Byte.MAX_VALUE;
        short theShort = Short.MAX_VALUE;
        long theLong = Long.MAX_VALUE;
        float theFloat = Float.MAX_VALUE;
        double theDouble = Double.MAX_VALUE;
        char theChar = 'a';
        boolean theBoolean = Boolean.TRUE;

        PrimitiveSerializer p = new PrimitiveSerializer();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        byte[] bytesString = theString.getBytes();
        assertEquals(theString, p.fromBytes(String.class, bytesString));

        dout.writeInt(theInt);
        byte[] bytesInt = bout.toByteArray();
        assertEquals(theInt, p.fromBytes(int.class, bytesInt));
        bout.reset();

        dout.writeByte(theByte);
        byte[] bytesByte = bout.toByteArray();
        assertEquals(theByte, p.fromBytes(byte.class, bytesByte));
        bout.reset();

        dout.writeShort(theShort);
        byte[] bytesShort = bout.toByteArray();
        assertEquals(theShort, p.fromBytes(short.class, bytesShort));
        bout.reset();

        dout.writeLong(theLong);
        byte[] bytesLong = bout.toByteArray();
        assertEquals(theLong, p.fromBytes(long.class, bytesLong));
        bout.reset();

        dout.writeFloat(theFloat);
        byte[] bytesFloat = bout.toByteArray();
        assertEquals(theFloat, p.fromBytes(float.class, bytesFloat));
        bout.reset();

        dout.writeDouble(theDouble);
        byte[] bytesDouble = bout.toByteArray();
        assertEquals(theDouble, p.fromBytes(double.class, bytesDouble));
        bout.reset();

        dout.writeChar(theChar);
        byte[] bytesChar = bout.toByteArray();
        assertEquals(theChar, p.fromBytes(char.class, bytesChar));
        bout.reset();

        dout.writeBoolean(theBoolean);
        byte[] bytesBoolean = bout.toByteArray();
        assertEquals(theBoolean, p.fromBytes(boolean.class, bytesBoolean));
        bout.reset();
    }
}