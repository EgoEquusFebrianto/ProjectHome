// Saat sebuah objek kita masukkan kedalam Spring container IoC, objek tersebut disebut Bean.
// Secara default Bean adalah Singleton Pattern

// untuk mengakses Bean kita perlu membuat method getBean
// getBean akan dieksekusi diawal sebelum method dipanggil kemudian disimpan di application context

package engineering.spring_pemula.core.bean;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import engineering.spring_pemula.core.bar.Animal;

@Slf4j
@Configuration
public class BeanConfiguration {

    @Bean(name = "dog")
    public Animal dog() {
        Animal dog = new Animal();
        log.info("Create new animal dog");
        return dog;
    }
}