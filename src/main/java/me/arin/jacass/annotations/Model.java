package me.arin.jacass.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Arin Sarkissian
 * Date: Mar 10, 2010
 * Time: 10:35:44 AM
 */

/**
 * Annotation denoting that a class should be persisted into Cassandra.
 * Also provides info as to where (Keyspace, CF, SC) to persist the Object 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Model {
    public String keyspace();

    public String columnFamily();

    public String superColumn();
}
