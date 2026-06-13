package lib.utils;

import lib.error.ScaneException;

public class errorExceptionUtils {
    public static void connectDatabase(String name, String password) {
        if (name == null || password == null) {
            throw new ScaneException("Cannot make connection to Database.");
        }
    }
}