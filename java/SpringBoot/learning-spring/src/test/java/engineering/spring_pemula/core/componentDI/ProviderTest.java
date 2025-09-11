package engineering.spring_pemula.core.componentDI;

import engineering.spring_pemula.core.componentDI.utilProvider.ProviderConfiguration;
import engineering.spring_pemula.core.componentDI.utilProvider.ProviderImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ProviderTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(ProviderImpl.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testProvider() {
        ProviderConfiguration beanProv = applicationContext.getBean(ProviderConfiguration.class);
        Assertions.assertEquals(3, beanProv.getAnimals().size());
    }
}
