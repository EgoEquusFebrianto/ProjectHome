package engineering.spring_pemula.core.beanValue;

import engineering.spring_pemula.core.bar.Animal;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class BeanValueConfiguration {

    @Primary
    @Bean(value = "1st-Animal")
    public Animal dog1b() {
        return new Animal();
    }

    @Bean(value = "2nd-Animal")
    public Animal dog2b() {
        return new Animal();
    }
}
