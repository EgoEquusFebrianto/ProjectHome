package lib.utils;

import lib.error.validationException;
import lib.models.LoginRequest;

public class throwAbleErrorUtils {
    public static void validated(LoginRequest loginRequest) throws validationException {
        if (loginRequest.username() == null) {
            throw new validationException("Username tidak boleh null");
        } else if (loginRequest.username().isBlank()) {
            throw new validationException("Username tidak boleh kosong");
        } else if (loginRequest.password() == null) {
            throw new validationException("Password tidak boleh null");
        } else if (loginRequest.password().isBlank()) {
            throw new validationException("Password tidak boleh kosong");
        }
    }

}