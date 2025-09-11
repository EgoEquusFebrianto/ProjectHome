package engineering.spring_pemula.core.componentDI;

import engineering.spring_pemula.core.componentDI.setterMethod.categoryImpl.CategoryImpl;
import engineering.spring_pemula.core.componentDI.setterMethod.categoryRepo.CategoryRepo;
import engineering.spring_pemula.core.componentDI.setterMethod.categoryService.CategoryService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ComponentDependencyInjectionSetterTest {
    private ConfigurableApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(CategoryImpl.class);
        applicationContext.registerShutdownHook();
    }

    @Test
    void testComponentSetter() {
        CategoryRepo categoryRepo = applicationContext.getBean(CategoryRepo.class);
        CategoryService categoryService = applicationContext.getBean(CategoryService.class);

        Assertions.assertSame(categoryRepo, categoryService.getCategoryRepo());
    }
}
