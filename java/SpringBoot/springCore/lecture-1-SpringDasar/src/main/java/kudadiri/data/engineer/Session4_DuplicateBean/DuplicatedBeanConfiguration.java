package kudadiri.data.engineer.Session4_DuplicateBean;

import kudadiri.data.engineer.data.Foo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DuplicatedBeanConfiguration {
    @Bean
    public Foo foo1() {
        return new Foo();
    }

    @Bean
    public Foo foo2() {
        return new Foo();
    }
}
