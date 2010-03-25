package me.arin.jacass.serializer;

import me.arin.jacass.JacassException;
import me.arin.jacass.Serializer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class PrimitiveSerializer implements Serializer {
    public static final Map<String, SuppportedType> typeMap = new HashMap<String, SuppportedType>();

    static {
        typeMap.put("java.lang.String", SuppportedType.STRING);
        typeMap.put("int", SuppportedType.INT);
        typeMap.put("java.lang.Integer", SuppportedType.INT);
        typeMap.put("byte", SuppportedType.BYTE);
        typeMap.put("java.lang.Byte", SuppportedType.BYTE);
        typeMap.put("short", SuppportedType.SHORT);
        typeMap.put("java.lang.Short", SuppportedType.SHORT);
        typeMap.put("long", SuppportedType.LONG);
        typeMap.put("java.lang.Long", SuppportedType.LONG);
        typeMap.put("float", SuppportedType.FLOAT);
        typeMap.put("java.lang.Float", SuppportedType.FLOAT);
        typeMap.put("double", SuppportedType.DOUBLE);
        typeMap.put("java.lang.Double", SuppportedType.DOUBLE);
        typeMap.put("char", SuppportedType.CHAR);
        typeMap.put("java.lang.Character", SuppportedType.CHAR);
        typeMap.put("boolean", SuppportedType.BOOLEAN);
        typeMap.put("java.lang.Boolean", SuppportedType.BOOLEAN);
    }

    public static SuppportedType getClassCode(Class cls) {
        return typeMap.get(cls.getName());
    }

    public byte[] toBytes(Class cls, Object value) throws JacassException {
        if (null == value) {
            return new byte[]{};
        }

        if ("java.lang.String".equals(cls.getName())) {
            return ((String) value).getBytes();
        }

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        try {
            SuppportedType classCode = PrimitiveSerializer.getClassCode(cls);
            if (classCode == null) {
                throw new JacassException("Unsupported type");
            }
            
            switch (classCode) {
                case INT:
                    dout.writeInt((Integer) value);
                    break;
                case BYTE:
                    Byte b = (Byte) value;
                    dout.writeByte(b.byteValue());
                    break;
                case SHORT:
                    Short s = (Short) value;
                    dout.writeShort(s.shortValue());
                    break;
                case LONG:
                    Long l = (Long) value;
                    dout.writeLong(l);
                    break;
                case FLOAT:
                    Float f = (Float) value;
                    dout.writeFloat(f);
                    break;
                case DOUBLE:
                    Double d = (Double) value;
                    dout.writeDouble(d);
                    break;
                case CHAR:
                    Character c = (Character) value;
                    dout.writeChar(c);
                    break;
                case BOOLEAN:
                    Boolean bool = (Boolean) value;
                    dout.writeBoolean(bool);
                    break;
                default:
                    dout.writeByte(0);
                    break;
            }

            dout.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bout.toByteArray();
    }

    public byte[] toBytes(Object value) throws JacassException {
        return toBytes(value.getClass(), value);
    }

    public Object fromBytes(byte[] bytes) throws IOException {
        return fromBytes(String.class, bytes);
    }

    public Object fromBytes(Class cls, byte[] bytes) throws IOException {
        if ("java.lang.String".equals(cls.getName())) {
            return new String(bytes);
        }

        Object castValue;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));

        switch (PrimitiveSerializer.getClassCode(cls)) {
            case INT:
                castValue = dis.readInt();
                break;
            case BYTE:
                castValue = dis.readByte();
                break;
            case SHORT:
                castValue = dis.readShort();
                break;
            case LONG:
                castValue = dis.readLong();
                break;
            case FLOAT:
                castValue = dis.readFloat();
                break;
            case DOUBLE:
                castValue = dis.readDouble();
                break;
            case CHAR:
                castValue = dis.readChar();
                break;
            case BOOLEAN:
                castValue = dis.readBoolean();
                break;
            default:
                return null;
        }

        return castValue;
    }
}
