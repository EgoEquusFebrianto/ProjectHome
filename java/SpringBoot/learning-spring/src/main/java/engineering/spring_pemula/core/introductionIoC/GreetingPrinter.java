// Penjelasan Annotations yang digunakan
// @Component: Menandai class agar dikelola oleh Bean. Cocok untuk class umum.

package engineering.spring_pemula.core.introductionIoC;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GreetingPrinter {
    private final GreetingService greetingService;

    @Autowired
    public GreetingPrinter(GreetingService greetingService) {
        this.greetingService = greetingService;
    }

    public void printGreeting() {
        System.out.println(greetingService.greet());
    }
}
