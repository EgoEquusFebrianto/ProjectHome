package engineering.spring_pemula.core.scopeSpring;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.scopes.scopeSpring.ScopeBeanConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ScopeBeanConfigurationTest {
    ApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(ScopeBeanConfiguration.class);
    }

    @Test
    void testScope() {
        Animal dog = applicationContext.getBean(Animal.class);
        Animal dog1 = applicationContext.getBean(Animal.class);
        Animal dog2 = applicationContext.getBean(Animal.class);

    }
}
