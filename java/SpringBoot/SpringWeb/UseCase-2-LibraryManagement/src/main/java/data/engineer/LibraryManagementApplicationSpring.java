package data.engineer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LibraryManagementApplicationSpring {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(LibraryManagementApplicationSpring.class);

        app.run(args);
    }
}