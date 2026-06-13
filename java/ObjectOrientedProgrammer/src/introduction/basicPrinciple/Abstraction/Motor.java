package introduction.basicPrinciple.Abstraction;

import introduction.basicPrinciple.Abstraction.based.Mesin;

public class Motor implements Mesin {
    @Override
    public void hidupkan() {
        System.out.println("Motor Dihidupkan.");
    }

    @Override
    public void matikan() {
        System.out.println("Motor Dimatikan.");
    }
}
