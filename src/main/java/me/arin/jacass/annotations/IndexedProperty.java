package me.arin.jacass.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Arin Sarkissian
 * Date: Mar 23, 2010
 * Time: 4:58:36 PM
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface IndexedProperty {
    public boolean unique();

    public boolean required();
}
