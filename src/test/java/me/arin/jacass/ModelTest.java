package me.arin.jacass;

import me.arin.jacass.serializer.testutil.EmbeddedServerHelper;
import me.arin.jacass.testModels.User;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;


/**
 * User: Arin Sarkissian
 * Date: Mar 24, 2010
 * Time: 2:19:39 PM
 */
public class ModelTest {
    private static EmbeddedServerHelper embedded;

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws org.apache.thrift.transport.TTransportException
     *
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException {
        embedded = new EmbeddedServerHelper(System.getProperty("user.dir"));
        embedded.setup();
        Executor.add("localhost", 9170);
    }

    @AfterClass
    public static void teardown() throws IOException {
        embedded.teardown();
    }

    @Test
    public void testBasicSave() throws Exception {
        String key = "random-key";
        String username = "phatduckk";
        String email = "phatduckk@example.com";

        User user = new User(username, email);
        user.setKey(key);
        user.save();

        User fetchedUser = (User) new User().load(key);
        assertNotNull(fetchedUser);
        assertTrue(fetchedUser instanceof User);
        assertEquals(key, fetchedUser.getKey());
        assertEquals(username, fetchedUser.getUsername());
        assertEquals(email, fetchedUser.getEmail());

        String changedEmail = "changedit@example.com";
        fetchedUser.setEmail(changedEmail);
        fetchedUser.save();

        User userPostEmailChange = (User) new User().load(key);
        assertEquals(changedEmail, userPostEmailChange.getEmail());
        userPostEmailChange.remove();

        User postRemove = (User) new User().load(key);
        assertNull(postRemove);
    }

    @Test
    public void testIndexSave() throws JacassException {
        User user = new User("username", "emailisindexed@example.com");
        user.save();

        ColumnKey ck = new ColumnKey(user.getKeyspace(), user.getColumnFamily(), "email.__unique__index__", user.getEmail());
        assertEquals(user.getKey(), Executor.get().getColumnCrud().getString(ck));

        User user2 = (User) new User().loadRef("email", user.getEmail());
        assertEquals(user.getKey(), user2.getKey());
    }

    @Test
    public void testKeyStuff() {
        User u = new User();
        assertNull(u.getKey());
        assertNotNull(u.getKey(true));

        String bsKey = "balls";
        User u2 = new User();

        u2.setKey(bsKey);
        assertEquals(bsKey, u2.getKey());
    }

    @Test
    public void testMultiget() throws Exception {
        User a = new User("username-a", "email-a");
        a.setKey("a");
        a.save();

        User b = new User("username-b", "email-b");
        b.setKey("b");
        b.save();

        Map<String, BaseModel> users = new User().load(new String[]{"a", "b", "c"});
        assertNotNull(users.get("a"));
        assertNotNull(users.get("b"));
        assertNull(users.get("c"));

        assertEquals("username-a", ((User) users.get("a")).getUsername());
        assertEquals("username-b", ((User) users.get("b")).getUsername());

        assertEquals("email-a", ((User) users.get("a")).getEmail());
        assertEquals("email-b", ((User) users.get("b")).getEmail());
    }

    @Test
    public void testColumnInfo() {
        User user = new User("username", "email");
        Map<String, ColumnInfo> columnInfo = user.getColumnInfos();

        ColumnInfo emailColumn = columnInfo.get("email");
        ColumnInfo usernameColumn = columnInfo.get("username");

        assertTrue(! columnInfo.isEmpty());
        assertNotNull(usernameColumn);
        assertNotNull(emailColumn);

        assertTrue(usernameColumn instanceof ColumnInfo);
        assertEquals("username", usernameColumn.getName());
        assertEquals(String.class, usernameColumn.getCls());
        assertNull(usernameColumn.getIndexData());
        assertFalse(usernameColumn.isIndexed());

        assertNotNull(emailColumn.getIndexData());
        assertTrue(emailColumn.getIndexData().isUnique());
        assertFalse(emailColumn.getIndexData().isRequired());
    }
}