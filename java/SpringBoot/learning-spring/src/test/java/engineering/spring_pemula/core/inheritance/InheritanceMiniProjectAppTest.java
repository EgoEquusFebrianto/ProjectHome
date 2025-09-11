package engineering.spring_pemula.core.inheritance;

import engineering.spring_pemula.core.inheritanceMiniProject.InheritanceMiniProjectApp;
import engineering.spring_pemula.core.inheritanceMiniProject.lib.PaymentProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class InheritanceMiniProjectAppTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(InheritanceMiniProjectApp.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testInheritanceApp() {
        PaymentProcessor payment = applicationContext.getBean(PaymentProcessor.class);
        payment.process("1000");
    }
}
