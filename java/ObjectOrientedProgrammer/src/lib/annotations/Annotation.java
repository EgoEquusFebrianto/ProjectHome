package lib.annotations;
import java.lang.annotation.*;

@Target(value = {ElementType.TYPE, ElementType.METHOD})
@Retention(value = RetentionPolicy.RUNTIME)
public @interface Annotation {
    String name();
    String[] tags() default {};
}