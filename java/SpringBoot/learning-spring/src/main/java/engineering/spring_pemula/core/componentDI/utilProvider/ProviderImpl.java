package engineering.spring_pemula.core.componentDI.utilProvider;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ComponentScan(basePackages = {
        "engineering.spring_pemula.core.configuration"
})
@Import(ProviderConfiguration.class)
public class ProviderImpl {
}