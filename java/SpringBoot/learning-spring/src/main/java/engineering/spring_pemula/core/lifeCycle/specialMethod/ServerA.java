package engineering.spring_pemula.core.lifeCycle.specialMethod;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerA {

    @PostConstruct
    public void start() {
        log.info("Server Start...");
    }

    @PreDestroy
    public void stop() {
        log.info("Server Stop...");
    }
}
