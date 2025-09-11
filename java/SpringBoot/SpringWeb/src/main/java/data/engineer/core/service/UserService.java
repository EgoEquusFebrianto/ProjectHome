package data.engineer.core.service;

import data.engineer.core.appRepository.AppRepository;
import data.engineer.core.data.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class UserService {
    private final AppRepository appRepository;

    public UserService(AppRepository appRepository) {
        this.appRepository = appRepository;
    }

    public List<User> getAll() {
        log.info("Permintaan Membaca Semua Data Masuk.");

        return appRepository.getUsers();
    }

    public Optional<User> getById(String id) {
        log.info("Permintaan Membaca Data Berdasarkan ID Masuk.");

        return appRepository
                .getUsers()
                .stream()
                .filter(value -> value.getId().equals(id))
                .findFirst();
    }

    public void addUser(User user) {
        appRepository.getUsers().add(user);
        log.info("User dengan data: {} | berhasil ditambahkan", user);
    }

    public void updateUser(String id, User user) {
        Optional<User> existingData = getById(id);
        if(existingData.isPresent()) {
            int index = appRepository.getUsers().indexOf(existingData.get());
            appRepository.getUsers().set(index, user);
            log.info("User dengan ID {} berhasil diupdate secara penuh.", id);
        }

//        int index = 0;
//        for (User data : appRepository.getUsers()) {
//            if(data.getId().equals(id)) {
//                appRepository.getUsers().set(index, user);
//                log.info("User dengan ID {} berhasil diupdate secara penuh.", id);
//                return;
//            }
//            index++;
//        }

        log.warn("User dengan ID {} tidak ditemukan.", id);
    }

    public void partialUpdateUser(String id, Map<String, Object> updates) {
        Optional<User> existingData = getById(id);
        if(existingData.isPresent()) {
            User user = existingData.get();
            updates.forEach((key, value) -> {
                    switch (key) {
                        case "name" -> user.setName((String) value);
                        case "age" -> user.setAge((Integer) value);
                        case "address" -> user.setAddress((String) value);
                        case "zipCode" -> user.setZipCode((String) value);
                        default -> log.warn("Field {} tidak dikenali untuk partial update", key);
                    }
                });
                log.info("User dengan ID {} berhasil diupdate sebagian dengan data: {}", id, updates);
        }

//        for (User user : appRepository.getUsers()) {
//            if (user.getId().equals(id)) {
//                updates.forEach((key, value) -> {
//                    switch (key) {
//                        case "name" -> user.setName((String) value);
//                        case "age" -> user.setAge((Integer) va    lue);
//                        case "address" -> user.setAddress((String) value);
//                        case "zipCode" -> user.setZipCode((String) value);
//                        default -> log.warn("Field {} tidak dikenali untuk partial update", key);
//                    }
//                });
//                log.info("User dengan ID {} berhasil diupdate sebagian dengan data: {}", id, updates);
//                return;
//            }
//        }

        log.warn("User dengan ID {} tidak ditemukan untuk partial update.", id);
    }

    public void deleteUser(String id) {
        Optional<User> existingData = getById(id);

        if (existingData.isPresent()) {

            int index = appRepository.getUsers().indexOf(existingData.get());
            appRepository.getUsers().remove(index);

            log.info("User with ID {} successfully remove from list", id);
        } else {
            log.warn("User with ID {} is not Found..", id);
        }
    }
}
