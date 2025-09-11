package engineering.spring_pemula.core.inheritanceMiniProject;

import engineering.spring_pemula.core.inheritanceMiniProject.lib.PaymentProcessor;
import engineering.spring_pemula.core.inheritanceMiniProject.paymentMethod.DanaPaymentService;
import engineering.spring_pemula.core.inheritanceMiniProject.paymentMethod.GoPayPaymentService;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
//@Import({
//        DanaPaymentService.class,
//        GoPayPaymentService.class,
//        PaymentProcessor.class
//})
@ComponentScan(basePackages = {
        "engineering.spring_pemula.core.inheritanceMiniProject.paymentMethod",
        "engineering.spring_pemula.core.inheritanceMiniProject.lib"
})
public class InheritanceMiniProjectApp {
}
