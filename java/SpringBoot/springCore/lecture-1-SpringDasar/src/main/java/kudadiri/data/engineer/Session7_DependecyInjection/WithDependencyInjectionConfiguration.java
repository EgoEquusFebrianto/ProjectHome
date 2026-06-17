package kudadiri.data.engineer.Session7_DependecyInjection;

import kudadiri.data.engineer.data.Fire;
import kudadiri.data.engineer.data.FireSpitter;
import kudadiri.data.engineer.data.Spitter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WithDependencyInjectionConfiguration {
    @Bean
    public Fire fire() {
        return new Fire();
    }

    @Bean
    public Spitter spitter() {
        return new Spitter();
    }

    @Bean
    public FireSpitter fireSpitter(Fire fire, Spitter spitter) {
        return new FireSpitter(fire, spitter);
    }
}
