package kudadiri.data.engineer.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import kudadiri.data.engineer.entity.User;
import kudadiri.data.engineer.service.UserServices;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RestController
@RequestMapping("/users")
@RequiredArgsConstructor
public class UserController {
    private final UserServices service;

    @GetMapping
    public List<User> getAllUsers() {
        return service.getAllUsers();
    }
    
    @GetMapping("/{id}")
    public ResponseEntity<?> getUserById(@PathVariable Long id) {
        return service.getUserById(id);
    }

    @GetMapping("/getByName/{name}")
    public ResponseEntity<?> getUserByName(@PathVariable String name) {
        return service.getUserByName(name);
    }

    @PostMapping("/create")
    public ResponseEntity<User> createUser(@RequestBody User user) {
        User userCreated = service.createUser(user);
        
        return ResponseEntity.status(HttpStatus.CREATED).body(userCreated);
    }

    @PutMapping("/update/{id}")
    public ResponseEntity<?> updateUser(@PathVariable Long id, @RequestBody User user) {
        return service.updateUser(id, user);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> deleteUser(@PathVariable Long id) {
        return service.deleteUser(id);
    }

}