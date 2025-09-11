package engineering.spring_pemula.core.factoryBean;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        FactoryBeanClientConfig.class
})
public class FactoryConfiguration {
}
