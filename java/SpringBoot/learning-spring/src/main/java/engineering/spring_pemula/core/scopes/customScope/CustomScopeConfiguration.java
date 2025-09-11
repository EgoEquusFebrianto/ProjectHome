package engineering.spring_pemula.core.scopes.customScope;

import engineering.spring_pemula.core.bar.Bar;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Slf4j
@Configuration
public class CustomScopeConfiguration {

    @Bean
    public CustomScopeConfigurer customScopeConfigurer() {
        CustomScopeConfigurer configurer = new CustomScopeConfigurer();
        configurer.addScope("doubleton", new CustomScope());
        return configurer;
    }

    @Bean
    @Scope(value = "doubleton")
    public Bar fish() {
        log.info("Create New Bar From CustomScope");
        return new Bar();
    }
}
