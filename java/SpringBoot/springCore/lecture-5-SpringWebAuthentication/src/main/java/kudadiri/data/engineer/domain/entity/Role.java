package kudadiri.data.engineer.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(
        schema = "spring_learning",
        name = "roles"
)
@Getter
@Setter
@NoArgsConstructor
public class Role extends BaseEntity{
    @Column(nullable = false, unique = true, length = 20)
    private String roleName;
}
