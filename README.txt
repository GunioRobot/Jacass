Take a look at ModelExample... but the jist is:

@Model(columnFamily = "Standard1", keyspace = "Keyspace1", superColumn = "")
public class User extends BaseModel {
    @ModelProperty
    String username;

    @ModelProperty
    String email;

    @ModelProperty
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

    @Override
    public String generateKey() {
        return UUIDGenerator.getInstance().generateRandomBasedUUID().toString();
    }
}

User u = new User("Arin Sarkissian", "arin@digg.com", 31);
u.save();

// load the user we just created & edit their email
User u2 = (User) new User().load(u.getKey());
u2.setEmail("newemail@example.com");
u2.save();