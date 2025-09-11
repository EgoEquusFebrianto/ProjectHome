package engineering.spring_pemula.core.commandlinerunnerApp;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CommandLineRunnerApplication {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(CommandLineRunnerApplication.class);
        application.setBannerMode(Banner.Mode.OFF);

        application.run(args);
    }
}
