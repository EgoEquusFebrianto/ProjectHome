package engineering.spring_pemula.core.lifeCycle.intro;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

@Slf4j
public class LifeCycleConfiguration implements InitializingBean, DisposableBean {
    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("Connection Database is ready...");
    }

    @Override
    public void destroy() throws Exception {
        log.info("Connection Database is closed successfully..");

    }
}
