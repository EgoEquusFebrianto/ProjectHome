package kudadiri.data.engineer.Session6_ReplaceBeanName;

import kudadiri.data.engineer.data.Foo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class ChangeBeanNameConfiguration {
    @Primary
    @Bean(value = "firstFoo") // nama harus unik
    public Foo foo1() {
        return new Foo();
    }

    @Bean(value = "secondFoo")
    public Foo foo2() {
        return new Foo();
    }
}
