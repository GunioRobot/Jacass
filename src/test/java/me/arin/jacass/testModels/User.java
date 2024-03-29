package me.arin.jacass.testModels;

import me.arin.jacass.BaseModel;
import me.arin.jacass.annotations.IndexedProperty;
import me.arin.jacass.annotations.Model;
import me.arin.jacass.annotations.SimpleProperty;

@Model(keyspace = "Keyspace1", columnFamily = "Standard1", superColumn = "")
public class User extends BaseModel {
    @SimpleProperty
    String username;

    @SimpleProperty
    @IndexedProperty(unique = true, required = false)
    String email;

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

    public User(String username, String email) {
        this.username = username;
        this.email = email;
    }

    public User() {
    }
}