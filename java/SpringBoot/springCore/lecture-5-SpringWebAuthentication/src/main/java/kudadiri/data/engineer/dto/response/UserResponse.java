package kudadiri.data.engineer.dto.response;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class UserResponse {
    private long id;
    private String fullName;
    private String email;
    private String phone;
    private String role;
}
