package engineering.spring_pemula.core.dependencyInjection;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.AnimalBar;
import engineering.spring_pemula.core.bar.Bar;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class WithDependencyInjection {

    @Primary
    @Bean
    public Animal cat1() {
        return new Animal();
    }

    @Bean
    public Animal cat2() {
        return new Animal();
    }

    @Bean
    public Bar fish1() { return new Bar();}

    @Bean
    public AnimalBar animalBar(@Qualifier("cat2") Animal cat, Bar fish1) {
        return new AnimalBar(cat, fish1);
    }
}