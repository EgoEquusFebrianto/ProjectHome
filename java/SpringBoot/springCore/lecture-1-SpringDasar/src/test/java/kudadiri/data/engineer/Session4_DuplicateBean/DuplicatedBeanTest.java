package kudadiri.data.engineer.Session4_DuplicateBean;

import kudadiri.data.engineer.data.Foo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class DuplicatedBeanTest {
    @Test
    void testDuplicateBean() {
        ApplicationContext context = new AnnotationConfigApplicationContext(DuplicatedBeanConfiguration.class);

        Foo foo1 = context.getBean("foo1", Foo.class);
        Foo foo2 = context.getBean("foo2", Foo.class);

        Assertions.assertNotSame(foo1, foo2);
    }
}
