package data.engineer.core.appRepository;

import data.engineer.core.data.User;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
public class AppRepository {

    @Getter
    private List<User> users;

    @PostConstruct
    public void init() {
        users = new ArrayList<>(List.of(
                new User("C01", "John Doe", 26, "Jl. Merdeka No. 123, Jakarta", "10110"),
                new User("C02", "Jane Doe", 25, "Jl. Sudirman Kav. 12, Jakarta", "10220"),
                new User("C03", "Dudung su Dudung", 27, "Jl. Pahlawan No. 45, Bandung", "40111"),
                new User("C04", "Jack MacTavish", 28, "Jl. Gatot Subroto No. 88, Surabaya", "60281"),
                new User("C05", "Arnold De'Brok", 24, "Jl. Asia Afrika No. 100, Bandung", "40112"),
                new User("C06", "Harold Michigan", 23, "Jl. Pemuda No. 10, Semarang", "50132"),
                new User("C07", "Acep su Acep", 27, "Jl. Cihampelas No. 15, Bandung", "40131"),
                new User("C08", "Lana Tri", 25, "Jl. Diponegoro No. 22, Yogyakarta", "55271"),
                new User("C09", "Joseph Stalin", 29, "Jl. Lenin No. 1, Moskow", "101000")
        ));
        log.info("[INFO] Data Telah Berhasil di Buat!");
    }
}