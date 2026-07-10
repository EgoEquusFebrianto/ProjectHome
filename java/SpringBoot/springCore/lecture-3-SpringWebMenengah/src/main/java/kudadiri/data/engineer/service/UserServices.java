package kudadiri.data.engineer.service;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import kudadiri.data.engineer.entity.User;
import kudadiri.data.engineer.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;

@Service
@RequiredArgsConstructor
public class UserServices {
    private final UserRepository repository;

    public List<User> getAllUsers() {
        return repository.findAll();
    }

    public ResponseEntity<?> getUserById(Long id) {
        return repository.findById(id)
            .<ResponseEntity<?>>map(ResponseEntity::ok)
            .orElse(
                ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("message", "User not found by id: " + id))
            );
    }

    public ResponseEntity<?> getUserByName(String name) {
        return repository.findByName(name)
            .<ResponseEntity<?>>map(ResponseEntity::ok)
            .orElse(
                ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("message", String.format("User with name %s is not found.", name)))
            );
    }

    public User createUser(User user) {
        return repository.save(user);
            
    }

    public ResponseEntity<?> updateUser(Long id, User user) {
        return repository.findById(id)
            .<ResponseEntity<?>>map(data -> {
                data.setName(user.getName());
                data.setEmail(user.getEmail());

                User updateUser = repository.save(data);

                return ResponseEntity.ok(updateUser);
            })
            .orElse(
                ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                        "message",
                        "User not found."
                    ))
            );
    }

    public ResponseEntity<?> deleteUser(@PathVariable Long id) {
        if (!repository.existsById(id)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "message",
                            "User not found."
                    ));
        }

        repository.deleteById(id);

        return ResponseEntity.noContent().build();
    }
}