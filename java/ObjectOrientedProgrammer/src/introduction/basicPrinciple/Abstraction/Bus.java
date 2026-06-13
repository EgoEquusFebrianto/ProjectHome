package introduction.basicPrinciple.Abstraction;

import introduction.basicPrinciple.Abstraction.based.Kendaraan;

class Bus extends Kendaraan {
    @Override
    public void berjalan() {
        System.out.println("Bus Berjalan.");
    }
}
