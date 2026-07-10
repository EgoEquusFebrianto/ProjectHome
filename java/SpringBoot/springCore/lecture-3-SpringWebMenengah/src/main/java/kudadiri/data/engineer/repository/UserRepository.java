package kudadiri.data.engineer.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import kudadiri.data.engineer.entity.User;

public interface  UserRepository extends JpaRepository<User, Long>{
    
    Optional<User> findByName(String name);

}