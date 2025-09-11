package engineering.spring_pemula.core.factoryBean;

import lombok.Data;

@Data
public class PaymentGateWay {
    private String endPoint;
    private String privateKey;
    private String publicKey;
}
