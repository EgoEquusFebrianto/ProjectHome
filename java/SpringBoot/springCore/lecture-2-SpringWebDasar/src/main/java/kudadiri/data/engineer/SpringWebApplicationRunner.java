package kudadiri.data.engineer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SpringWebApplicationRunner implements ApplicationRunner {
    @Value("${app.name}")
    private String appName;

    @Override
    public void run(ApplicationArguments args) throws Exception {
       log.info("The Session right now is {}", appName);
    }
}