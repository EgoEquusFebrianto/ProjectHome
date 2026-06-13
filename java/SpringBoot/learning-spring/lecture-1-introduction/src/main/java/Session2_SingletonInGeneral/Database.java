// Pengenalan Singleton Pattern pada Spring Boot

package Session2_SingletonInGeneral;

public class Database {
    private static Database database;

    public static Database getInstance() {
        if (database == null) {
            database = new Database();
        }

        return database;
    }

    private Database() {}
}
