package lib.inheritance._extends;

import lib.inheritance.Person;

public class VicePresident extends Person {

    String name;
    Integer price;

    public VicePresident(String _name, String _room) {
        super(_name, _room);
    }

    @Override
    public void sayHello(String data) {
        System.out.println("Hello " + data + ", I am " + fullName + ", The Vice President");
    }

    
}
