package engineering.spring_pemula.core.dependencyInjection;

import engineering.spring_pemula.core.bar.Animal;
import engineering.spring_pemula.core.bar.AnimalBar;
import engineering.spring_pemula.core.bar.Bar;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class DependencyInjectionTest {

    ApplicationContext applicationContext;

    @BeforeEach
    void setUp() {
        applicationContext = new AnnotationConfigApplicationContext(WithDependencyInjection.class);
    }

    @Test
    void testDI() {
        Animal foo = applicationContext.getBean("cat2", Animal.class);
        Bar moo = applicationContext.getBean(Bar.class);
        AnimalBar fooMoo = applicationContext.getBean(AnimalBar.class);

        Assertions.assertSame(foo, fooMoo.getDog());
        Assertions.assertSame(moo, fooMoo.getFish());
    }

    @Test
    void testNoDI() {
        var foo = new Animal();
        var moo = new Bar();

        var fooMoo = new AnimalBar(foo, moo);

        Assertions.assertSame(foo, fooMoo.getDog());
        Assertions.assertSame(moo, fooMoo.getFish());
    }
}