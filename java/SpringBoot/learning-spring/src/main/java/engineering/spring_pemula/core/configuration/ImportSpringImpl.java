package engineering.spring_pemula.core.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        AnimalConfig.class,
        BarConfig.class
})
public class ImportSpringImpl {
}