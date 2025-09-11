package engineering.spring_pemula.core.duplicateBean;

import engineering.spring_pemula.core.bar.Animal;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DuplicateConfiguration {

    @Bean
    public Animal dog1a() {
        return new Animal();
    }

    @Bean
    public Animal dog2a() {
        return new Animal();
    }
}
