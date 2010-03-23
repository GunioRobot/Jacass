package me.arin.util;

/**
 * User: arin
 * Date: Mar 3, 2010
 * Time: 9:04:29 PM
 */
public class StringUtil {
    public static String ucFirst(String str) {
        if (str == null) {
            return null;
        }

        if ("".equals(str)) {
            return "";
        }

        if (str.length() == 1) {
            return str.toUpperCase();
        }

        return new String(String.valueOf(str.charAt(0))).toUpperCase() + str.substring(1);
    }
}
