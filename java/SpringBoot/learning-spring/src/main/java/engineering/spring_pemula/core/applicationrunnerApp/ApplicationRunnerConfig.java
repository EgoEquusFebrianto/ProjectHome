package engineering.spring_pemula.core.applicationrunnerApp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Slf4j
@Component
public class ApplicationRunnerConfig implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Set<String> optionNames = args.getOptionNames();
        List<String> nonOptionArgs = args.getNonOptionArgs();
        List<String> optionValues1 = args.getOptionValues("password");
        List<String> optionValues2 = args.getOptionValues("name");

        log.info("Application Runner Called");
        log.info("optionNames: {}", optionNames);
        log.info("nonOptionArgs: {}", nonOptionArgs);
        log.info("optionValues-1: {}", optionValues1);
        log.info("optionValues-2: {}", optionValues2);
    }
}
