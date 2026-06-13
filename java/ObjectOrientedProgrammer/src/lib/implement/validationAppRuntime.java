package lib.implement;

import lib.models.LoginRequest;
import lib.utils.runtimeErrorUtils;

public class validationAppRuntime {
    public static void main(String[] args) {
        LoginRequest loginRequest = new LoginRequest(null, null);
        runtimeErrorUtils.validatedRuntime(loginRequest);
        System.out.println("testing");
    }
}
