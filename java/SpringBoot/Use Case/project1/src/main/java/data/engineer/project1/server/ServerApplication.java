package data.engineer.project1.server;

import data.engineer.project1.serverConf.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class ServerApplication {
    @Bean(value = "serverA")
    public Server serverA(Environment env) {
        String id = env.getProperty("servera-id", "A0");
        String name = env.getProperty("servera-name", "Server A");
        boolean status = Boolean.parseBoolean(env.getProperty("servera-status", "true"));
        return new Server(id, name, status);
    }

    @Bean(value = "serverB")
    public Server serverB(Environment env) {
        String id = env.getProperty("serverb-id", "B0");
        String name = env.getProperty("serverb-name", "Server B");
        boolean status = Boolean.parseBoolean(env.getProperty("serverb-status", "false"));
        return new Server(id, name, status);
    }
}