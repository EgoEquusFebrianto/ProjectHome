package introduction.basicPrinciple.Abstraction;


import introduction.basicPrinciple.Abstraction.based.Kendaraan;
import introduction.basicPrinciple.Abstraction.based.Mesin;

public class AbstractImplement {
    public static void main(String[] args) {

        // Abstract Class Implementation
        Kendaraan bus1 = new Bus();
        Kendaraan mobil1 = new Mobil();

        Bus bus2 = new Bus();
        Mobil mobil2 = new Mobil();

        mobil1.berjalan();
        bus1.berjalan();

        mobil2.berjalan();
        bus2.berjalan();

        // Interface Implementation
        Mesin motor = new Motor();
        motor.hidupkan();
        motor.matikan();
    }
}
