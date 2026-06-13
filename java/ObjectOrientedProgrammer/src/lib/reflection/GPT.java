package lib.reflection;

import java.lang.reflect.Method;
import lib.annotations.Annotation;

public class GPT {
    public static void main(String[] args) {
        try {
            Class<?> obj = MyClass.class;

            // Cek apakah class memiliki annotation
            if (obj.isAnnotationPresent(Annotation.class)) {
                Annotation annotation = obj.getAnnotation(Annotation.class);
                System.out.println("Class Annotation Name: " + annotation.name());
            }

            // Cek apakah method memiliki annotation
            for (Method method : obj.getDeclaredMethods()) {
                if (method.isAnnotationPresent(Annotation.class)) {
                    Annotation annotation = method.getAnnotation(Annotation.class);
                    System.out.println("Method: " + method.getName());
                    System.out.println("Annotation Name: " + annotation.name());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

@Annotation(name = "ExampleClass", tags = {"feature", "v1.0"})
class MyClass {
    @Annotation(name = "ExampleMethod")
    public void exampleMethod() {}
}

