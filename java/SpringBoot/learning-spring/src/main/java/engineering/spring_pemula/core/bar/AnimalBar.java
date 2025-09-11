package engineering.spring_pemula.core.bar;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AnimalBar {
    private Animal dog;
    private Bar fish;
}