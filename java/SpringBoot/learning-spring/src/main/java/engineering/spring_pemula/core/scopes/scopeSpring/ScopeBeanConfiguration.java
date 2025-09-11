package engineering.spring_pemula.core.scopes.scopeSpring;

import engineering.spring_pemula.core.bar.Animal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Slf4j
@Configuration
public class ScopeBeanConfiguration {

    @Bean
    @Scope(value = "prototype")
    public Animal dogA() {
        log.info("Create New Dog");
        return new Animal();
    }

}
