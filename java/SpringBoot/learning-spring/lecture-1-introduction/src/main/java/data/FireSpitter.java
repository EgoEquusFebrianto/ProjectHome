package data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FireSpitter {
    private Fire fire;
    private Spitter spitter;
}