package engineering.spring_pemula.core.componentDI;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.AnimalBar;
import engineering.spring_pemula.core.componentDI.utilOptional.OptionalConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class OptionalTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(OptionalConfiguration.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testOptional() {
        Animal animal = applicationContext.getBean(Animal.class);
        AnimalBar animalBar = applicationContext.getBean(AnimalBar.class);

        Assertions.assertNull(animalBar.getFish());
        Assertions.assertSame(animal, animalBar.getDog());
    }
}
