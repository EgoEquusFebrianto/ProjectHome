package engineering.spring_pemula.core.inheritance;

import engineering.spring_pemula.core.inheritance.Impl.MerchantServiceImpl;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        MerchantServiceImpl.class
})
public class InheritanceConfig {
}