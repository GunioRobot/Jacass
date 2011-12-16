package me.arin.jacass;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * User: Arin Sarkissian
 * Date: Mar 27, 2010
 * Time: 1:04:13 PM
 */
public class RowPathTest{
    @Test
    public void testGettersNSetters() {
        RowPath rp = new RowPath("ks", "cf", "sc");
        assertEquals("ks", rp.getKeyspace());
        assertEquals("cf", rp.getColumnFamily());
        assertEquals("sc", rp.getSuperColumn());

        rp.setKeyspace("ks2");
        rp.setColumnFamily("cf2");
        rp.setSuperColumn("sc2");

        assertEquals("ks2", rp.getKeyspace());
        assertEquals("cf2", rp.getColumnFamily());
        assertEquals("sc2", rp.getSuperColumn());
    }
}
