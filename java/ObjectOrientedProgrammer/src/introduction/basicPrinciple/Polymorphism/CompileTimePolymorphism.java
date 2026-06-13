package introduction.basicPrinciple.Polymorphism;

import introduction.basicPrinciple.Polymorphism.classes.Calculator;

public class CompileTimePolymorphism {
    public static void main(String[] args) {
        Calculator calculator = new Calculator();

        System.out.println(calculator.add(2, 5));
        System.out.println(calculator.add(2.0, 5.0));
        System.out.println(calculator.add(2, 3, 5));
    }
}
