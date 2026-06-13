package introduction.basicPrinciple.Polymorphism.classes;

public class Anjing extends Hewan{

    String ras;

    @Override
    public void bersuara() {
        super.bersuara();
        System.out.println();
    }
}
