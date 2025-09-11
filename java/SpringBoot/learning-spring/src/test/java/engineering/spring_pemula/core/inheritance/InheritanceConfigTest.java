package engineering.spring_pemula.core.inheritance;

import engineering.spring_pemula.core.inheritance.Impl.MerchantServiceImpl;
import engineering.spring_pemula.core.inheritance.services.MerchantService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class InheritanceConfigTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(InheritanceConfig.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testInheritance() {
        MerchantService service = applicationContext.getBean(MerchantService.class);
        MerchantServiceImpl serviceImpl = applicationContext.getBean(MerchantServiceImpl.class);

        Assertions.assertSame(service, serviceImpl);
    }
}
