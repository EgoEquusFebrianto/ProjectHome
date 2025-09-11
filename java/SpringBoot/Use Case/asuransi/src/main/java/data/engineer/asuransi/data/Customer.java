package data.engineer.asuransi.data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Customer {
    private String name;
    private String noKTP;
    private int umur;
    private String type;
}