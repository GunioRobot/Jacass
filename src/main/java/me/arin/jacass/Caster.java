package me.arin.jacass;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

enum SuppportedType {
    INT,
    BYTE,
    SHORT,
    LONG,
    FLOAT,
    DOUBLE,
    CHAR,
    BOOLEAN,
    STRING
}

class Caster {
    public static final Map<String, SuppportedType> typeMap = new HashMap<String, SuppportedType>();

    static {
        typeMap.put("java.lang.String", SuppportedType.STRING);
        typeMap.put("int", SuppportedType.INT);
        typeMap.put("byte", SuppportedType.BYTE);
        typeMap.put("short", SuppportedType.SHORT);
        typeMap.put("long", SuppportedType.LONG);
        typeMap.put("float", SuppportedType.FLOAT);
        typeMap.put("double", SuppportedType.DOUBLE);
        typeMap.put("char", SuppportedType.CHAR);
        typeMap.put("boolean", SuppportedType.BOOLEAN);
    }

    public static SuppportedType getClassCode(Class cls) {
        return typeMap.get(cls.getName());
    }

    public static byte[] toBytes(Class columnType, Object value) {
        if (null == value) {
            return new byte[]{};
        }

        if ("java.lang.String".equals(columnType.getName())) {
            return ((String) value).getBytes();
        }

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        try {
            switch (Caster.getClassCode(columnType)) {
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
                    return null;
            }

            dout.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bout.toByteArray();
    }

    public static Object fromBytes(Class columnType, byte[] bytesValue) throws IOException {
        if ("java.lang.String".equals(columnType.getName())) {
            return new String(bytesValue);
        }

        Object castValue;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytesValue));

        switch (Caster.getClassCode(columnType)) {
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
