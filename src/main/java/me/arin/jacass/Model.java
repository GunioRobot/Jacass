package me.arin.jacass;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: arin
 * Date: Mar 10, 2010
 * Time: 10:35:44 AM
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Model {
    public String keyspace();

    public String columnFamily();

    public String superColumn();
}
