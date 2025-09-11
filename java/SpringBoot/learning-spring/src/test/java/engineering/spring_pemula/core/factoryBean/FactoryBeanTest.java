package engineering.spring_pemula.core.factoryBean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class FactoryBeanTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(FactoryConfiguration.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testFactoryBean() {
        PaymentGateWay client = applicationContext.getBean(PaymentGateWay.class);
        Assertions.assertEquals("https://example.com", client.getEndPoint());
        Assertions.assertEquals("public", client.getPublicKey());
        Assertions.assertEquals("private", client.getPrivateKey());
    }
}
