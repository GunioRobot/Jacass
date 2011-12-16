package me.arin.jacass;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * User: Arin Sarkissian
 * Date: Mar 27, 2010
 * Time: 1:08:09 PM
 */
public class ColumnKeyTest {
    @Test
    public void testGettersnSetters() {
        ColumnKey ck = new ColumnKey("ks", "sc", "cf", "key", "col");

        assertEquals("ks", ck.getKeyspace());
        assertEquals("sc", ck.getSuperColumn());
        assertEquals("cf", ck.getColumnFamily());
        assertEquals("key", ck.getKey());
        assertEquals("col", ck.getColumnName());

        ck.setColumnFamily("cf2");
        ck.setColumnName("col2");
        ck.setKey("key2");
        ck.setKeyspace("ks2");
        ck.setSuperColumn("sc2");

        assertEquals("ks2", ck.getKeyspace());
        assertEquals("sc2", ck.getSuperColumn());
        assertEquals("cf2", ck.getColumnFamily());
        assertEquals("key2", ck.getKey());
        assertEquals("col2", ck.getColumnName());
    }
}
