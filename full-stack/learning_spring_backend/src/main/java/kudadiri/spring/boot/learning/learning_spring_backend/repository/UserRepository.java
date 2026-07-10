package kudadiri.spring.boot.learning.learning_spring_backend.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import kudadiri.spring.boot.learning.learning_spring_backend.entity.User;

public interface  UserRepository extends JpaRepository<User, Long>{
    
    Optional<User> findByName(String name);

}