package kudadiri.data.engineer.Session8_MemilihDependency;

import kudadiri.data.engineer.data.Fire;
import kudadiri.data.engineer.data.FireSpitter;
import kudadiri.data.engineer.data.Spitter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

public class SelectDependencyConfiguration {
    @Primary
    @Bean
    public Fire fireFirst() {
        return new Fire();
    }

    @Bean
    public Fire fireSecond() {
        return new Fire();
    }

    @Bean
    public Spitter spitter() {
        return new Spitter();
    }

    @Bean
    public FireSpitter fireSpitter(@Qualifier("fireSecond") Fire fire, Spitter spitter) {
        return new FireSpitter(fire, spitter);
    }
}
