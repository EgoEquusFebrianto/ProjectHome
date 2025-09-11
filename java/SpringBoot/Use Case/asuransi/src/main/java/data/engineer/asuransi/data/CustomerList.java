package data.engineer.asuransi.data;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Service
public class CustomerList {

    @Getter
    private List<Customer> customers;

    @PostConstruct
    public void init() {
        customers = new ArrayList<>(Arrays.asList(
                new Customer("Acep", "123123123123", 23, "vehicle"),
                new Customer("Budi", "234234234234", 25, "life"),
                new Customer("Citra", "345345345345", 30, "health"),
                new Customer("Dewi", "456456456456", 28, "vehicle"),
                new Customer("Eka", "567567567567", 35, "life"),
                new Customer("Fajar", "678678678678", 40, "health"),
                new Customer("Gita", "789789789789", 22, "vehicle"),
                new Customer("Hadi", "890890890890", 29, "life"),
                new Customer("Indra", "901901901901", 31, "health"),
                new Customer("Joko", "012012012012", 27, "vehicle"),
                new Customer("Kartika", "123456789012", 33, "life"),
                new Customer("Lina", "234567890123", 26, "health")
        ));
    }

    public void addCustomers(Customer customer) {
        customers.add(customer);
    }

    public boolean searchCustomer(String name, String noKTP) {
        for (Customer customer : customers ) {
            if (Objects.equals(customer.getName(), name) && Objects.equals(customer.getNoKTP(), noKTP)) {
                return true;
            }
        }
        return false;
    }
}