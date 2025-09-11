package engineering.spring_pemula.core.primaryBean;

import engineering.spring_pemula.core.bar.Animal;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class PrimaryBeanConfiguration {
    @Primary
    @Bean
    public Animal dog1c() {
        return new Animal();
    }

    @Bean
    public Animal dog2c() {
        return new Animal();
    }
}
