package engineering.spring_pemula.core.componentDI.getterMethod.productImpl;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
        "engineering.spring_pemula.core.componentDI.getterMethod.productStorage",
        "engineering.spring_pemula.core.componentDI.getterMethod.componentDIInitialize"
})
public class ComponentDIImpl {
}