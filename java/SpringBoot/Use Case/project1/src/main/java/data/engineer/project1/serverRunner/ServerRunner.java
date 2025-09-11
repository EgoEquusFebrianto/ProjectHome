package data.engineer.project1.serverRunner;

import data.engineer.project1.serverConf.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ServerRunner implements ApplicationRunner {

    private final Server serverA;
    private final Server serverB;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Serve Information:");
        log.info("Server A -> ID: {}, Name: {}, Active: {}", serverA.getId(), serverA.getName(), serverA.isStatus());
        log.info("Server B -> ID: {}, Name: {}, Active: {}", serverB.getId(), serverB.getName(), serverB.isStatus());
    }
}