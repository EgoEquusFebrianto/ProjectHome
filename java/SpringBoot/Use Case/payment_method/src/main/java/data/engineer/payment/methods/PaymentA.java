package data.engineer.payment.methods;

import data.engineer.payment.PaymentGateAway;
import data.engineer.payment.identifier.PaymentConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component("0")
public class PaymentA implements PaymentGateAway {

    @Override
    public void info(PaymentConfig paymentConfig) {
        log.info("[INFO-A] Payment {} for {} processed via {}, with amounts {}",
                paymentConfig.getId(),
                paymentConfig.getName(),
                paymentConfig.getMethodName(),
                paymentConfig.getAmount()
        );
    }
}