package me.arin.examples;

import me.arin.jacass.BaseModel;
import me.arin.jacass.annotations.Model;
import me.arin.jacass.annotations.SimpleProperty;

/**
 * User: Arin Sarkissian
 * Date: Mar 22, 2010
 * Time: 4:45:07 PM
 */

@Model(columnFamily = "Standard1", keyspace = "Keyspace1", superColumn = "")
public class User extends BaseModel {
    @SimpleProperty
    String username;

    @SimpleProperty
    String email;

    @SimpleProperty
    int age;

    public User() {
    }

    public User(String username, String email, int age) {
        this.username = username;
        this.email = email;
        this.age = age;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
