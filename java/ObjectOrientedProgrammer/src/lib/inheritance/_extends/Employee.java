package lib.inheritance._extends;

import lib.inheritance.Person;

public class Employee extends Person {

    public Employee(String _fullName, String _room) {
        super(_fullName, _room);
    }

    @Override
    public void sayHello(String data) {
        System.out.println("Hello " + data + ", I am " + fullName + ", An Employee");
    }
}
