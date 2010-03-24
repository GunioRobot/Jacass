package me.arin.jacass;

import me.arin.jacass.serializer.testutil.EmbeddedServerHelper;
import me.arin.jacass.testModels.User;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

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
        Executor.add("Keyspace1", "localhost", 9170);
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
}