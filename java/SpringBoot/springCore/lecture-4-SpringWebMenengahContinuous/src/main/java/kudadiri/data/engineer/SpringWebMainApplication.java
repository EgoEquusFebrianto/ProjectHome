package kudadiri.data.engineer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringWebMainApplication {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SpringWebMainApplication.class);

        app.run(args);
    }
}