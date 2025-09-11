package data.engineer.payment.identifier;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PaymentConfig {
    private String id;
    private String name;
    private long amount;
    private String method;

    public String getMethodName() {
        return switch (this.method) {
            case "0" -> "QRIS";
            case "1" -> "Dana";
            case "2" -> "GoPay";
            default -> "Unknown";
        };
    }
}