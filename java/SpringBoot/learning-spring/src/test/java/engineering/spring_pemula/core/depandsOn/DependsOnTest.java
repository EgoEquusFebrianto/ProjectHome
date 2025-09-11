package engineering.spring_pemula.core.depandsOn;

import engineering.spring_pemula.core.depansOn.DependsConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class DependsOnTest {

    ApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(DependsConfiguration.class);
    }

    @Test
    void testDependsOn() {

    }
}
