package engineering.spring_pemula.core.inheritanceMiniProject.lib;

import engineering.spring_pemula.core.inheritanceMiniProject.payment.PaymentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class PaymentProcessor {
    private final PaymentService paymentService;

    @Autowired
    public PaymentProcessor(@Qualifier("goPay") PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    public void process(String amount) {
        paymentService.pay(amount);
    }
}