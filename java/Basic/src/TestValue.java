import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestValue {
    public static void main(String[] args) {
        Map<String, String> cars = new HashMap<>();

        cars.put("Toyota", "XUV");
        cars.put("Hyundai", "GTR-40");

        System.out.println(cars.get("Hyundai"));

        cars.replace("Hyundai", "XRV");
        System.out.println(cars.get("Hyundai"));
    }
}
