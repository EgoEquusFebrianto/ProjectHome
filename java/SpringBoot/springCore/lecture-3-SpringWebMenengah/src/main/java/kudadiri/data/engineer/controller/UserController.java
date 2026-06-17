package kudadiri.data.engineer.controller;

import kudadiri.data.engineer.entity.User;
import kudadiri.data.engineer.repository.UserRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class UserController {
    private final UserRepository repository;

    public UserController(UserRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/users")
    public List<User> getUsers() {
        return repository.findAll();
    }
}
