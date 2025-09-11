package data.engineer.asuransi;

import data.engineer.asuransi.data.Customer;
import data.engineer.asuransi.data.InsuranceClaim;

public interface ClaimHandler {
    public void processClaim(InsuranceClaim insuranceClaim, String noKTP);
    public void addCustomer(Customer customer);
}