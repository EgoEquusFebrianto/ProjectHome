package engineering.spring_pemula.core.bean;

import engineering.spring_pemula.core.bar.Animal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@SpringBootTest
public class BeanTest {
    ApplicationContext context;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigApplicationContext(BeanConfiguration.class);
    }

    @Test
    void testCreateBean() {
        Assertions.assertNotNull(context);
    }

    @Test
    void testGetBean() {
        Animal dog1 = context.getBean(Animal.class);
        Animal dog2 = context.getBean(Animal.class);

        Assertions.assertSame(dog1, dog2);
    }
}
