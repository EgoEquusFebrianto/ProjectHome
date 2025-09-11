package engineering.spring_pemula.core.componentScan;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
        "engineering.spring_pemula.core.configuration"
})
public class ComponentScanConfig {
}
