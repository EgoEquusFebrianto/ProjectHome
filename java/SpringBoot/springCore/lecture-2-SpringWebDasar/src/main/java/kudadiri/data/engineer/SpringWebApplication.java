package kudadiri.data.engineer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;

@SpringBootApplication
public class SpringWebApplication {
	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(SpringWebApplication.class);
//		app.setDefaultProperties(Collections.singletonMap("spring.profiles.active", "dev"));

		app.run(args);
	}
}