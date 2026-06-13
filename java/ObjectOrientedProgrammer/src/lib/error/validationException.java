package lib.error;

public class validationException extends Throwable{
    public validationException(String message) {
        super(message);
    }
}