package Session7_DependecyInjection;

import data.Fire;
import data.FireSpitter;
import data.Spitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ManualDependencyInjectionTest {
    @Test
    void testManualDI() {
        Fire fire = new Fire();
        Spitter spitter = new Spitter();

        FireSpitter fireSpitter = new FireSpitter(fire, spitter);

        Assertions.assertSame(fireSpitter.getFire(), fire);
        Assertions.assertSame(fireSpitter.getSpitter(), spitter);

    }
}
