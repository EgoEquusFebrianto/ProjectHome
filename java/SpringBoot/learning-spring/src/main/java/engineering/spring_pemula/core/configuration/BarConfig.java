package engineering.spring_pemula.core.configuration;

import engineering.spring_pemula.core.bar.Bar;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BarConfig {
    @Bean
    public Bar bar() {
        return new Bar();
    }
}
