package data.engineer.asuransi.data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class InsuranceClaim {
    private String policyId;
    private String customerName;
    private String claimAmount;
    private String type;
}