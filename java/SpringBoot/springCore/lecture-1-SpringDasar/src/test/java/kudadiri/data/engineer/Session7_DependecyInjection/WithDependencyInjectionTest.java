package kudadiri.data.engineer.Session7_DependecyInjection;

import kudadiri.data.engineer.data.Fire;
import kudadiri.data.engineer.data.FireSpitter;
import kudadiri.data.engineer.data.Spitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class WithDependencyInjectionTest {
    ApplicationContext context;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigApplicationContext(WithDependencyInjectionConfiguration.class);
    }

    @Test
    void testWithDI() {
        Fire fire = context.getBean(Fire.class);
        Spitter spitter = context.getBean(Spitter.class);
        FireSpitter fireSpitter = context.getBean(FireSpitter.class);

        Assertions.assertSame(fireSpitter.getFire(), fire);
        Assertions.assertSame(fireSpitter.getSpitter(), spitter);
    }
}
