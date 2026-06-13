package Session7_DependecyInjection;

import data.Fire;
import data.Spitter;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ManualDependencyInjectionConfiguration {
    private Fire fire;
    private Spitter spitter;
}