package engineering.spring_pemula.core.componentConfiguration;

import engineering.spring_pemula.core.componentAnnotation.ComponentAnnotationConfig;
import engineering.spring_pemula.core.componentConfig.ComponentConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ComponentConfigTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(ComponentConfiguration.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void name() {
        ComponentAnnotationConfig componentTest1 = applicationContext.getBean(
                ComponentAnnotationConfig.class
        );

        ComponentAnnotationConfig componentTest2 = applicationContext.getBean(
                "componentAnnotationConfig",
                ComponentAnnotationConfig.class
        );

        Assertions.assertSame(componentTest1, componentTest2);
    }
}
