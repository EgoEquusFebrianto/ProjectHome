package engineering.spring_pemula.core.lifeCycle;

import engineering.spring_pemula.core.lifeCycle.intro.LifeCycleConfiguration;
import engineering.spring_pemula.core.lifeCycle.intro.LifeCycleImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class LifeCycleTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(LifeCycleImpl.class);
        applicationContext.registerShutdownHook(); // demografi otomatis destroy(),
    }

    // Bukan Implementasi yang baik ini hanya eksperimen dan pengenalan
    @AfterEach
    void tearDown() {
//        applicationContext.close(); // demografi Manual destroy()
    }

    @Test
    void testLifeCycle() {
        LifeCycleConfiguration life = applicationContext.getBean(LifeCycleConfiguration.class);
    }
}
