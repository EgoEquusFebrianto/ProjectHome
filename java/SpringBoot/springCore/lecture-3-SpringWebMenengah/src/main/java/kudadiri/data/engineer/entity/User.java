package kudadiri.data.engineer.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(
        name = "users",
        schema = "spring_learning"
)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {
    @Id
    private Long id;
    private String name;
    private String email;
}