package engineering.spring_pemula.core.inheritanceMiniProject.paymentMethod;

import engineering.spring_pemula.core.inheritanceMiniProject.payment.PaymentService;
import org.springframework.stereotype.Component;

@Component(value = "goPay")
public class GoPayPaymentService implements PaymentService {
    @Override
    public void pay(String amount) {
        System.out.println("Paying " + amount + " using GoPay");
    }
}
