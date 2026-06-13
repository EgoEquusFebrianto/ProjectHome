package lib.utils;

import lib.error.BlankException;
import lib.models.LoginRequest;

public class runtimeErrorUtils {
    public static void validatedRuntime(LoginRequest loginRequest) {
        if (loginRequest.username() == null) {
            throw new NullPointerException("Username tidak boleh null");
        } else if (loginRequest.username().isBlank()) {
            throw new BlankException("Username tidak boleh kosong");
        } else if (loginRequest.password() == null) {
            throw new NullPointerException("Password tidak boleh null");
        } else if (loginRequest.password().isBlank()) {
            throw new BlankException("Password tidak boleh kosong");
        }
    }
}