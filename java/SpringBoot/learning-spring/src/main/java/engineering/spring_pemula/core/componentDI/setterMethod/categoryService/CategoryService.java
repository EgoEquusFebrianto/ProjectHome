package engineering.spring_pemula.core.componentDI.setterMethod.categoryService;

import engineering.spring_pemula.core.componentDI.setterMethod.categoryRepo.CategoryRepo;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CategoryService {

    @Getter
    private CategoryRepo categoryRepo;

    @Autowired
    public void setCategoryRepo(CategoryRepo categoryRepo) {
        this.categoryRepo = categoryRepo;
    }
}
