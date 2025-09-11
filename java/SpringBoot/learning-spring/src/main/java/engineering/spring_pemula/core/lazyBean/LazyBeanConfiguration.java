package engineering.spring_pemula.core.lazyBean;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.Bar;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Slf4j
@Configuration
public class LazyBeanConfiguration {

    @Lazy
    @Bean(name = "1st-chicken")
    public Animal chicken0() {
        log.info("Animal Bean for chicken is created...");
        return new Animal();
    }

    @Bean(name = "1st-bird")
    public Bar bird0() {
        log.info("Bar Bean for bird is created...");
        return new Bar();
    }
}
