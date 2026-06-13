package lib.reflection;

public class implementReflection {
    public static void main(String[] args) {
        CreateUser request = new CreateUser();
        request.setUsername("silent");
        request.setPassword("listen");
        ErrorCheck.validatedWithReflection(request);

    }
}
