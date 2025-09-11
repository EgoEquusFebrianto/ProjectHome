package engineering.spring_pemula.core.componentDI.getterMethod.componentDIInitialize;

import engineering.spring_pemula.core.componentDI.getterMethod.productStorage.ProductRepo;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProductService {

    @Getter
    private ProductRepo productRepo;

    @Autowired
    public ProductService(ProductRepo productRepo) {
        this.productRepo = productRepo;
    }

    public ProductService(ProductRepo productRepo, String name) {
        this.productRepo = productRepo;
    }
}
