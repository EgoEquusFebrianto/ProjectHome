package data.engineer.project1;

import data.engineer.project1.events.StartedListener;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProjectApp {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(ProjectApp.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.addListeners(new StartedListener());
		app.run(args);
	}
}