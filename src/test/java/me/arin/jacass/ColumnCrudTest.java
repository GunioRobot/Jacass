package me.arin.jacass;

import me.arin.jacass.serializer.testutil.EmbeddedServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

/**
 * User: Arin Sarkissian
 * Date: Mar 24, 2010
 * Time: 6:25:55 PM
 */
public class ColumnCrudTest {
    private static EmbeddedServerHelper embedded;

    public String theString = "theString";
    public int theInt = Integer.MAX_VALUE;
    public byte theByte = Byte.MAX_VALUE;
    public short theShort = Short.MAX_VALUE;
    public long theLong = Long.MAX_VALUE;
    public float theFloat = Float.MAX_VALUE;
    public double theDouble = Double.MAX_VALUE;
    public char theChar = 'a';
    public boolean theBoolean = Boolean.TRUE;

    static ColumnCrud cc;

    @BeforeClass
    public static void setUp() throws Exception {
        embedded = new EmbeddedServerHelper(System.getProperty("user.dir"));
        embedded.setup();
        cc = new ColumnCrud(Executor.add("Keyspace1", "localhost", 9170));
    }

    private ColumnKey getColumnKey(String columnName) {
        return new ColumnKey("Keyspace1", "Standard1", "crudtest", columnName);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        embedded.teardown();
    }

    @Test
    public void testSetAndGetAndRemoveStandard() throws Exception {
        String[] varNameBase = new String[]{"String", "Int", "Byte", "Short", "Long", "Float", "Double", "Char", "Boolean"};

        ColumnKey columnKey = getColumnKey(null);
        for (String s : varNameBase) {
            Field field = getClass().getField("the" + s);
            columnKey.setColumnName(field.getName());
            cc.set(columnKey, field.get(this));
        }

        assertEquals(theString, cc.getString(getColumnKey("theString")));
        assertEquals(theInt, cc.getInt(getColumnKey("theInt")));
        assertEquals(theByte, cc.getByte(getColumnKey("theByte")));
        assertEquals(theShort, cc.getShort(getColumnKey("theShort")));
        assertEquals(theLong, cc.getLong(getColumnKey("theLong")));
        assertEquals(theFloat, cc.getFloat(getColumnKey("theFloat")), 0);
        assertEquals(theDouble, cc.getDouble(getColumnKey("theDouble")), 0);
        assertEquals(theChar, cc.getChar(getColumnKey("theChar")));
        assertEquals(theBoolean, cc.getBoolean(getColumnKey("theBoolean")));
    }

    @Test
    public void testGetWithDefaultValues() throws Exception {
        assertEquals(theString, cc.getString(getColumnKey("bsString"), theString));
        assertEquals(theInt, cc.getInt(getColumnKey("bsInt"), theInt));
        assertEquals(theByte, cc.getByte(getColumnKey("bsByte"),theByte));
        assertEquals(theShort, cc.getShort(getColumnKey("bsShort"), theShort));
        assertEquals(theLong, cc.getLong(getColumnKey("bsLong"), theLong));
        assertEquals(theFloat, cc.getFloat(getColumnKey("bsFloat"), theFloat), 0);
        assertEquals(theDouble, cc.getDouble(getColumnKey("bsDouble"), theDouble), 0);
        assertEquals(theChar, cc.getChar(getColumnKey("bsChar"), theChar));
        assertEquals(theBoolean, cc.getBoolean(getColumnKey("bsBoolean"), theBoolean));        
    }
}
