package engineering.spring_pemula.core.duplicateBean;

import engineering.spring_pemula.core.bar.Animal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@SpringBootTest
public class DuplicateTest {
    @Test
    void testDuplicate() {
        ApplicationContext context = new AnnotationConfigApplicationContext(DuplicateConfiguration.class);

        Animal dog1 = context.getBean("dog1", Animal.class);
        Animal dog2 = context.getBean("dog2", Animal.class);

        Assertions.assertNotSame(dog1, dog2);
    }
}
