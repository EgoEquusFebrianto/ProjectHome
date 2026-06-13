import lib.inheritance._extends.Employee;
import lib.inheritance._extends.Manager;
import lib.inheritance._extends.VicePresident;

public class Main {
    public static void main(String[] args) {
        var person2 = new Manager("Shaddam", "B47");
        person2.sayHello("Golok");

        var person3 = new VicePresident("Hussain", "BB4");
        person3.sayHello("Bisctahsuk");

        var person4 = new Employee("Laginna La Varvalla", "Spa7");
        person4.sayHello("Golok");

    }

}