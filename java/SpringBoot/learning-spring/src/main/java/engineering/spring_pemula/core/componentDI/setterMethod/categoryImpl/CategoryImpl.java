package engineering.spring_pemula.core.componentDI.setterMethod.categoryImpl;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
        "engineering.spring_pemula.core.componentDI.setterMethod.categoryService",
        "engineering.spring_pemula.core.componentDI.setterMethod.categoryRepo"
})
public class CategoryImpl {
}
