package me.arin.examples;

import me.arin.jacass.BaseModel;
import me.arin.jacass.annotations.IndexedProperty;
import me.arin.jacass.annotations.Model;
import me.arin.jacass.annotations.ModelProperty;
import org.safehaus.uuid.UUIDGenerator;

/**
 * User: Arin Sarkissian
 * Date: Mar 23, 2010
 * Time: 9:40:32 PM
 */

@Model(keyspace = "Keyspace1", columnFamily = "Standard1", superColumn = "")
public class Employee extends BaseModel {
    @ModelProperty
    String name;

    @IndexedProperty(unique = false, required = false)
    String department;

    public Employee(String name, String department) {
        this.name = name;
        this.department = department;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }
}
