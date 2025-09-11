package engineering.spring_pemula.core.componentDI;

import engineering.spring_pemula.core.componentDI.getterMethod.componentDIInitialize.ProductService;
import engineering.spring_pemula.core.componentDI.getterMethod.productImpl.ComponentDIImpl;
import engineering.spring_pemula.core.componentDI.getterMethod.productStorage.ProductRepo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ComponentDependencyInjectionGetterTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(ComponentDIImpl.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testComponentDI() {
        ProductRepo product = applicationContext.getBean(ProductRepo.class);
        ProductService componentProduct = applicationContext.getBean(ProductService.class);

        Assertions.assertSame(product, componentProduct.getProductRepo());
    }
}
