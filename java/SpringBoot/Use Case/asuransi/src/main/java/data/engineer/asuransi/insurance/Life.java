package data.engineer.asuransi.insurance;

import data.engineer.asuransi.ClaimHandler;
import data.engineer.asuransi.data.Customer;
import data.engineer.asuransi.data.CustomerList;
import data.engineer.asuransi.data.InsuranceClaim;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
@Component("life")
public class Life implements ClaimHandler {

    @Autowired
    private CustomerList customerList;

    @Override
    public void addCustomer(Customer customer) {
        customerList.addCustomers(customer);
    }

    @Override
    public void processClaim(InsuranceClaim insuranceClaim, String noKTP) {
        if (!insuranceClaim.getPolicyId().isEmpty() && Objects.equals(insuranceClaim.getType(), "life")) {
            if (customerList.searchCustomer(insuranceClaim.getCustomerName(), noKTP)) {
                log.info("Claim will already receive.. Please wait until it's done..");
            } else {
                log.info("Customer not recognized, please try again..");
            }
        } else {
            log.warn("Data Invalid..");
        }
    }
}