package lib.implement;

import lib.algorithm.CompanyStatic;
import static lib.algorithm.staticBlock.PROCESSORS;

public class implementInnerCompanyStatic {
    public static void main(String[] args) {
        CompanyStatic.DepartmentStatic company = new CompanyStatic.DepartmentStatic();
        company.setName("Kosong");
        System.out.println(company.getName());

        System.out.println(PROCESSORS);
    }
}
