package lib.implement;

import lib.error.validationException;
import lib.models.LoginRequest;
import lib.utils.throwAbleErrorUtils;

public class validationAppThrowable {
    public static void main(String[] args) {
        LoginRequest loginRequest = new LoginRequest(null, null);
        try {
            throwAbleErrorUtils.validated(loginRequest);
            System.out.println("Data valid.");
        } catch (validationException e) {
            System.out.println("Error Catch: " + e.getMessage());
        }
    }
}