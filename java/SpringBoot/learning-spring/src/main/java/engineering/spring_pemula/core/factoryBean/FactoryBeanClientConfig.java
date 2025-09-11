package engineering.spring_pemula.core.factoryBean;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.stereotype.Component;

@Component(value = "paymentGateWay")
public class FactoryBeanClientConfig implements FactoryBean<PaymentGateWay> {
    @Override
    public PaymentGateWay getObject() throws Exception {
        PaymentGateWay client = new PaymentGateWay();
        client.setEndPoint("https://example.com");
        client.setPrivateKey("private");
        client.setPublicKey("public");
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return PaymentGateWay.class;
    }
}