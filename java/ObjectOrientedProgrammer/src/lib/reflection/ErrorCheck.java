package lib.reflection;

import lib.annotations.NotBlank;
import lib.error.ScaneException;
import java.lang.reflect.Field;

public class ErrorCheck {
    public static void validatedWithReflection(CreateUser loginRequest) {
        Field[] fields = loginRequest.getClass().getDeclaredFields();
        for(Field field : fields) {
            if (field.getAnnotation(NotBlank.class) != null) {
                field.setAccessible(true);
                try {
                    String value = (String) field.get(loginRequest);
                    if (value == null || value.isBlank()) {
                        throw new ScaneException("Field " + field.getName() + " is blank.");
                    }
                } catch (IllegalAccessException e ) {
                    System.out.println("TIdak bisa mengakses field " + e.getMessage());
                }
            }
        }
    }
}
