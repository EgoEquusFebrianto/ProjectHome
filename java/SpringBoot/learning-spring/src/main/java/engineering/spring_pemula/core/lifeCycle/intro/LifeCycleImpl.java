package engineering.spring_pemula.core.lifeCycle.intro;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LifeCycleImpl {

    @Bean
    public LifeCycleConfiguration connection() {
        return new LifeCycleConfiguration();
    }
}
