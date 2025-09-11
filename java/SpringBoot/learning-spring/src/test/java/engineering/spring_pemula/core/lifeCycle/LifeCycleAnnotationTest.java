package engineering.spring_pemula.core.lifeCycle;

import engineering.spring_pemula.core.lifeCycle.annotationWay.LifeCycleServer;
import engineering.spring_pemula.core.lifeCycle.annotationWay.Server;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class LifeCycleAnnotationTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(LifeCycleServer.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testServer() {
        Server server = applicationContext.getBean(Server.class);
    }
}
