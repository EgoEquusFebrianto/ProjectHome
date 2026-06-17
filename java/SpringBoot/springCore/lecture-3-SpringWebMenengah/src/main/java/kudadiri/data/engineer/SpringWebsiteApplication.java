package kudadiri.data.engineer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringWebsiteApplication {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SpringWebsiteApplication.class);

        app.run(args);
    }
}