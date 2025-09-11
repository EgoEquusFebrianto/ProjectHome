package engineering.spring_pemula.core.lifeCycle.specialMethod;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LifeCycleSpecialConf {

    @Bean
    public ServerA serverA() {
        return new ServerA();
    }
}
