package engineering.spring_pemula.core.lifeCycle;

import engineering.spring_pemula.core.lifeCycle.specialMethod.LifeCycleSpecialConf;
import engineering.spring_pemula.core.lifeCycle.specialMethod.ServerA;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class LifeCycleSpecialTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(LifeCycleSpecialConf.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testLifeCycleSpecialTest() {
        ServerA props = applicationContext.getBean(ServerA.class);
    }
}
