package engineering.spring_pemula.core.beanValue;

import engineering.spring_pemula.core.bar.Animal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class BeanValueTest {
    private ApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(BeanValueConfiguration.class);
    }

    @Test
    void testBeanValue() {
        Animal dog = applicationContext.getBean(Animal.class);
        Animal dog1 = applicationContext.getBean("1st-Animal", Animal.class);
        Animal dog2 = applicationContext.getBean("2nd-Animal",Animal.class);

        Assertions.assertSame(dog, dog1);
        Assertions.assertNotSame(dog, dog2);

    }
}
