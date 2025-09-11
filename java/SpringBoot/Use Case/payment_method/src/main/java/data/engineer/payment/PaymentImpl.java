package data.engineer.payment;

import data.engineer.payment.identifier.PaymentConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import java.util.List;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class PaymentImpl implements ApplicationRunner {

    private final Map<String, PaymentGateAway> paymentGateAwayMap;

    // @Autowired
    // public PaymentImpl(Map<String, PaymentGateAway> paymentGateAwayMap) {
    //     this.paymentGateAwayMap = paymentGateAwayMap;
    // }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<PaymentConfig> payments = List.of(
                new PaymentConfig("001", "John Doe", 100000, "0"),
                new PaymentConfig("002", "Jane Smith", 150000, "1"),
                new PaymentConfig("003", "Bob Johnson", 200000, "2")
        );

        for (PaymentConfig config : payments) {
            PaymentGateAway gateway = paymentGateAwayMap.get(config.getMethod());
            if (gateway != null) {
                gateway.info(config);
            } else {
                System.out.println("Unknown payment method: " + config.getMethod());
            }
        }
    }
}