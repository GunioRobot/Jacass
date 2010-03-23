package me.arin.examples;

/**
 * User: Arin Sarkissian
 * Date: Mar 22, 2010
 * Time: 4:44:38 PM
 */
public class ModelExample {
    public static void main(String[] args) {
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
    }
}
