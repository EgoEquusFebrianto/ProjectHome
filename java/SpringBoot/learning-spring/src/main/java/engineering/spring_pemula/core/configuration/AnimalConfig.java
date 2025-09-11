package engineering.spring_pemula.core.configuration;

import engineering.spring_pemula.core.bar.Animal;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AnimalConfig {

    @Primary
    @Bean
    public Animal animal() {
        return new Animal();
    }

    @Bean
    public Animal animal1() {
        return new Animal();
    }

    @Bean
    public Animal animal2() {
        return new Animal();
    }
}
