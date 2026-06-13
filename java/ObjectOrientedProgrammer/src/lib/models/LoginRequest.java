package lib.models;

public class LoginRequest {
    private String user;
    private String pass;

    public LoginRequest(String _username, String _password) {
        user = _username;
        pass = _password;
    }

    public String username() {
        return user;
    }

    public String password() {
        return pass;
    }
}
