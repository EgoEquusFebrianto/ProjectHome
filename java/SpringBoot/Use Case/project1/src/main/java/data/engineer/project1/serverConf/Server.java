package data.engineer.project1.serverConf;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Server {
    private String id;
    private String name;
    private boolean status;
}
