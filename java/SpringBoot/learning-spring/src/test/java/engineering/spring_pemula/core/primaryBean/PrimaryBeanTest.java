package engineering.spring_pemula.core.primaryBean;

import engineering.spring_pemula.core.bar.Animal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class PrimaryBeanTest {
    private ApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(PrimaryBeanConfiguration.class);
    }

    @Test
    void testPrimaryBean() {
        Animal dog = applicationContext.getBean(Animal.class);
        Animal dog1 = applicationContext.getBean("dog1", Animal.class);
        Animal dog2 = applicationContext.getBean("dog2", Animal.class);

        Assertions.assertSame(dog, dog1);
        Assertions.assertNotSame(dog, dog2);
        Assertions.assertNotSame(dog1, dog2);
    }
}
