package methods;

public class Method_Overloading {
    // method overloading adalah kemampuan membuat method lain dengan nama yang sama
    // dengan syarat, parameter method harus berbeda, jika tidak maka akan error

    static void sayHello() {
        System.out.println("Hello boys");
    }

    static  void sayHello(String firstName) {
        System.out.println("Hello " + firstName);
    }

    static  void sayHello(String firstName, String lastName) {
        System.out.println("Hello " + firstName + " " + lastName);
    }

    public static void main(String[] args) {
        sayHello();
        sayHello("Febrianto");
        sayHello("Febrianto", "Kudadiri");
    }
}
