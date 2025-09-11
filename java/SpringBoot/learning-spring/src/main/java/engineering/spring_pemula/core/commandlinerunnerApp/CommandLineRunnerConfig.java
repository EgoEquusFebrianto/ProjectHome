package engineering.spring_pemula.core.commandlinerunnerApp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
public class CommandLineRunnerConfig implements CommandLineRunner {

    @Override
    public void run(String... args) throws Exception {
        log.info("Program Arguments are {}", Arrays.toString(args));
    }
}