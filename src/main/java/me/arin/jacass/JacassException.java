package me.arin.jacass;

/**
 * User: Arin Sarkissian
 * Date: Mar 23, 2010
 * Time: 2:52:15 PM
 */
public class JacassException extends Exception {
    public JacassException() {
    }

    public JacassException(String message) {
        super(message);
    }

    public JacassException(String message, Throwable cause) {
        super(message, cause);
    }

    public JacassException(Throwable cause) {
        super(cause);
    }
}
