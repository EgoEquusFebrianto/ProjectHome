package lib.implement;
import lib.algorithm.Company;

class implementInnerClass {
    public static void main(String[] args) {
        Company company = new Company();
        company.setName("Teradog");

        Company.Department department = company.new Department();
        department.setName("Golok");

        System.out.println(company.getName());
        System.out.println(department.getName());

    }

}
