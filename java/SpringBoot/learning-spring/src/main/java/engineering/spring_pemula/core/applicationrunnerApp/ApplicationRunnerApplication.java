package engineering.spring_pemula.core.applicationrunnerApp;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApplicationRunnerApplication {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ApplicationRunnerApplication.class);
        application.setBannerMode(Banner.Mode.OFF);

        application.run(args);
    }
}
