package me.arin.jacass;

/**
 * User: Arin Sarkissian
 * Date: Mar 22, 2010
 * Time: 4:06:19 PM
 */
public interface Serializer {
    public byte[] toBytes(Object o);
    public Object fromBytes(byte[] bytes);
}
