package engineering.spring_pemula.core.lifeCycle.annotationWay;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server {
    public void start() {
        log.info("Start Server...");
    }

    public void stop() {
        log.info("Stop Server...");
    }
}
