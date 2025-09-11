package data.engineer.project1.events;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ReadyListener{
    @EventListener
    public void onReady(ApplicationReadyEvent event) {
        log.info("Application is ready to receive request...");
    }
}