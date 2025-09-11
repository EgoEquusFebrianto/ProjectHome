package engineering.spring_pemula.core.inheritanceMiniProject.payment;

import org.springframework.stereotype.Component;

@Component
public interface PaymentService {
    void pay(String amount);
}
