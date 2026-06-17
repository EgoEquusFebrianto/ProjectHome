package kudadiri.data.engineer.Session6_ReplaceBeanName;

import kudadiri.data.engineer.data.Foo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ChangeBeanNameTest {
    private ApplicationContext context;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigApplicationContext(ChangeBeanNameConfiguration.class);
    }

    @Test
    void testChangeBeanName() {
        Object fooPrimary = context.getBean(Foo.class);
        Object foo1 = context.getBean("firstFoo", Foo.class);
        Object foo2 = context.getBean("secondFoo", Foo.class);

        Assertions.assertSame(fooPrimary, foo1);
        Assertions.assertNotSame(fooPrimary, foo2);
    }
}
