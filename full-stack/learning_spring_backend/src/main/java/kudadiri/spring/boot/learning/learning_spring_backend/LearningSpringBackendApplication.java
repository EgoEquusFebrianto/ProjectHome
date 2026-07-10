package kudadiri.spring.boot.learning.learning_spring_backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LearningSpringBackendApplication {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(LearningSpringBackendApplication.class);

		app.run(args);
	}
}