// Penjelasan Annotations yang digunakan
// @Service: Sama seperti Component, tapi digunakan khusus untuk class service layer.

package engineering.spring_pemula.core.introductionIoC;

import org.springframework.stereotype.Service;

@Service
public class GreetingServiceImpl implements GreetingService {

    @Override
    public String greet() {
        return "Hello, this is IoC demo";
    }
}
