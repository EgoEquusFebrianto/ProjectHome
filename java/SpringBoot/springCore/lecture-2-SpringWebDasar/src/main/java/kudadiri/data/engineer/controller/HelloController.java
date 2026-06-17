package kudadiri.data.engineer.controller;

import kudadiri.data.engineer.data.User;
import kudadiri.data.engineer.service.UserService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/users")
public class HelloController {

    @Autowired
    private UserService userService;

    @GetMapping
    public List<User> show() {
        return userService.getAll();
    }

    @GetMapping("/{id}")
    public Optional<User> showById(@PathVariable String id) {
        return userService.getById(id);
    }

    @PostMapping("/add")
    public ResponseEntity<String> addUser(@RequestBody @Valid User user) {
        userService.addUser(user);
        return ResponseEntity.status(HttpStatus.CREATED).body("User Berhasil Ditambahkan");
    }

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PutMapping("/update/{id}")
    public ResponseEntity<String> updateUser(@PathVariable String id, @RequestBody User user) {
        userService.updateUser(id, user);

        String message = "Data User dengan ID " + id + " Berhasil di Update Seluruhnya.";
        return ResponseEntity.ok(message);
    }

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PatchMapping("/partialUpdate/{id}")
    public ResponseEntity<String> patchUser(@PathVariable String id , @RequestBody Map<String, Object> user) {
        userService.partialUpdateUser(id, user);

        String message = "Data User dengan ID " + id + " Berhasil di Update.";
        return  ResponseEntity.ok(message);
    }

    @DeleteMapping("/delete/{id}")
    public void deleteUser(@PathVariable String id) {
        userService.deleteUser(id);
    }
}
