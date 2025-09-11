package data.engineer.asuransi;

import data.engineer.asuransi.eventListener.StartedListener;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InsuranceApplication {

	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(InsuranceApplication.class);
		application.addListeners(new StartedListener());

		application.setBannerMode(Banner.Mode.OFF);
		application.run(args);
	}
}