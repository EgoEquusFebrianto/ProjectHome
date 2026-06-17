package kudadiri.data.engineer.Session8_MemilihDependency;

import kudadiri.data.engineer.data.Fire;
import kudadiri.data.engineer.data.FireSpitter;
import kudadiri.data.engineer.data.Spitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class SelectDependencyTest {
    private ApplicationContext context;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigApplicationContext(SelectDependencyConfiguration.class);
    }

    @Test
    void testSelectDependency() {
        Fire fire = context.getBean(Fire.class);
        Spitter spitter = context.getBean(Spitter.class);
        FireSpitter fireSpitter = context.getBean(FireSpitter.class);

        // gagal krena fire diambil dari primary bean (fireFirst) sedangkan Fire dari firespitter diambil dari bean fireSecond
        Assertions.assertSame(fireSpitter.getFire(), fire);
        Assertions.assertSame(fireSpitter.getSpitter(), spitter);
    }
}
