package kudadiri.data.engineer.Session1_Intro;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ApplicationContextTest {
    @Test
    void testApplicationContext() {
        ApplicationContext context = new AnnotationConfigApplicationContext(HelloWorldConfiguration.class);

        Assertions.assertNotNull(context);
    }
}