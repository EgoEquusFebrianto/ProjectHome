package engineering.spring_pemula.core.componentConfig;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
        "engineering.spring_pemula.core.componentAnnotation"
})
public class ComponentConfiguration {
}
