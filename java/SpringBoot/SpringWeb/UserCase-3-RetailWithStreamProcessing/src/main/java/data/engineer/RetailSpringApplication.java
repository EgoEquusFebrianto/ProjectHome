package data.engineer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RetailSpringApplication {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(RetailSpringApplication.class);

        app.run(args);
    }
}