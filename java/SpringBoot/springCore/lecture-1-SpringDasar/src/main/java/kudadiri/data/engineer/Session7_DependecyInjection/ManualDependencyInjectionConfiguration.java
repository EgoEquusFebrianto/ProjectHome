package kudadiri.data.engineer.Session7_DependecyInjection;

import kudadiri.data.engineer.data.Fire;
import kudadiri.data.engineer.data.Spitter;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ManualDependencyInjectionConfiguration {
    private Fire fire;
    private Spitter spitter;
}