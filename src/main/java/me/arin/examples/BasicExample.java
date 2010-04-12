package me.arin.examples;

import me.arin.jacass.BaseModel;
import me.arin.jacass.Executor;
import me.arin.jacass.JacassException;

import java.util.Map;

/**
 * User: Arin Sarkissian
 * Date: Mar 22, 2010
 * Time: 4:44:38 PM
 */

public class BasicExample {
    public static void main(String[] args) throws JacassException {
        // initialze the cassandra connection info
        Executor.add("Keyspace1", "localhost", 9160);

        // create and save a new user
        User u = new User("Arin Sarkissian", "arin@digg.com", 31);
        u.save();

        // load up the user we created into a new object, change their email and persist the change
        User u2 = (User) new User().load(u.getKey(), true);
        u2.setEmail("newemail@example.com");
        u2.save();

        // OK - create a new, distinct user in cassandra
        User u3 = new User("Username 2", "email2@example.com", 666);
        u3.save();

        // now do a multiget on the 2 distinct users and spit out their info        
        Map<String, BaseModel> userMap = new User().load(new String[]{u3.getKey(), u.getKey()});
        for (String key : userMap.keySet()) {
            User user = (User) userMap.get(key);

            System.out.println("User key: " + key);
            System.out.println("\temail: " + user.getEmail());
            System.out.println("\tusername: " + user.getUsername());
        }
    }
}
