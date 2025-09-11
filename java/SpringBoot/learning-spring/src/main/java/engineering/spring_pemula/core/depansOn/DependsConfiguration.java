package engineering.spring_pemula.core.depansOn;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.Bar;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration
public class DependsConfiguration {

    @Bean(name = "dog0")
    @DependsOn({
            "fish0"
    })
    public Animal dag0() {
        log.info("Create New animal dawg..");
        return new Animal();
    }

    @Bean(name = "fish0")
    public Bar fish0() {
        log.info("Create New bar fish..");
        return new Bar();
    }
}
