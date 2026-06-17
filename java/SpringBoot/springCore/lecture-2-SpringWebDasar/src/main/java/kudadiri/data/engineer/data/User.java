package kudadiri.data.engineer.data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class User {
    private String id;
    private String name;
    private int age;
    private String address;
    private String zipCode;
}