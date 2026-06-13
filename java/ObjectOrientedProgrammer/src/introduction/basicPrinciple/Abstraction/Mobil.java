package introduction.basicPrinciple.Abstraction;

import introduction.basicPrinciple.Abstraction.based.Kendaraan;

class Mobil extends Kendaraan {
    @Override
    public void berjalan() {
        System.out.println("Mobil Berjalan.");
    }
}
