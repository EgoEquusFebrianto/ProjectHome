package kudadiri.data.engineer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

@SpringBootApplication
@EnableJpaAuditing
public class SpringAuthenticationApp {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SpringAuthenticationApp.class);
        app.run(args);
    }
}