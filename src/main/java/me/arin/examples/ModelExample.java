package me.arin.examples;

import me.arin.jacass.BaseModel;
import me.arin.jacass.Executor;

import java.util.Map;

/**
 * User: Arin Sarkissian
 * Date: Mar 22, 2010
 * Time: 4:44:38 PM
 */
public class ModelExample {
    public static void main(String[] args) {
        Executor.add("Keyspace1", "localhost", 9160);

        User u = new User("Arin Sarkissian", "arin@digg.com", 31);
        u.save();
        System.out.println("created a new user");
        System.out.println("\tuuid key: " + u.getKey());

        System.out.println("\nloading user w/ row key: " + u.getKey());
        User u2 = (User) new User().load(u.getKey());
        System.out.println("\tkey: " + u2.getKey());
        System.out.println("\temail: " + u2.getEmail());
        System.out.println("\tusername: " + u2.getUsername());

        System.out.println("changing their email");
        u2.setEmail("newemail@example.com");
        u2.save();
        System.out.println("\temail: " + u2.getEmail());

        System.out.println("\nloading user w/ row key: " + u.getKey());
        User u3 = new User();
        u3.load(u.getKey());
        System.out.println("\tkey: " + u3.getKey());
        System.out.println("\temail: " + u3.getEmail());
        System.out.println("\tusername: " + u3.getUsername());

        User u4 = new User("Username 2", "email2@example.com", 666);
        u4.save();

        Map<String,BaseModel> userMap = new User().get(new String[]{u4.getKey(), u.getKey()});
        System.out.println("hello");
    }
}
