package engineering.spring_pemula.core.configuration;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.Bar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ImportSpringTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(ImportSpringImpl.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testImportSpring() {
        Animal animal = applicationContext.getBean(Animal.class);
        Bar bar = applicationContext.getBean(Bar.class);
    }
}
