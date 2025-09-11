package engineering.spring_pemula.core.componentDI.utilOptional;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.AnimalBar;
import engineering.spring_pemula.core.bar.Bar;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

@Configuration
public class OptionalConfiguration {

    @Bean
    public Animal chicken1() {
        return new Animal();
    }

    @Bean
    public AnimalBar animalBar1(@Qualifier("chicken1") Optional<Animal> animal, Optional<Bar> bar) {
        return new AnimalBar(animal.orElse(null), bar.orElse(null));
    }

}
