package engineering.spring_pemula.core.componentScan;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.Bar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.reactive.context.AnnotationConfigReactiveWebApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

public class SpringScanTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigReactiveWebApplicationContext(ComponentScanConfig.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testSpringScan() {
        Animal animal = applicationContext.getBean(Animal.class);
        Bar bar = applicationContext.getBean(Bar.class);
    }
}
