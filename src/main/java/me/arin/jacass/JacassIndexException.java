package me.arin.jacass;

/**
 * User: Arin Sarkissian
 * Date: Mar 31, 2010
 * Time: 10:23:30 PM
 */
public class JacassIndexException extends JacassException {
    public JacassIndexException(String s, Exception e) {
        super(s, e);
    }

    public JacassIndexException(String s) {
        super(s);
    }
}
