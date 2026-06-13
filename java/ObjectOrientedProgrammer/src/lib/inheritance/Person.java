package lib.inheritance;

public abstract class Person {
    public String fullName;
    public String room;

    public Person(String _fullName, String _room) {
        fullName = _fullName;
        room = _room;
    }

    public abstract void sayHello(String data);
}
