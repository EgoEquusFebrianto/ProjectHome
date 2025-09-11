package engineering.spring_pemula.core.componentDI.utilProvider;

import engineering.spring_pemula.core.bar.Animal;
import lombok.Getter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ProviderConfiguration {

    @Getter
    private List<Animal> animals;

    public ProviderConfiguration(ObjectProvider<Animal> objectProvider) {
        animals = objectProvider.stream().collect(Collectors.toList());
    }
}