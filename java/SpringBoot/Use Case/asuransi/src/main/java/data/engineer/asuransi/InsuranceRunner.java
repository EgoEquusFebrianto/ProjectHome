package data.engineer.asuransi;

import data.engineer.asuransi.data.Customer;
import data.engineer.asuransi.data.InsuranceClaim;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class InsuranceRunner implements ApplicationRunner {

    @Autowired
    private Map<String, ClaimHandler> claimHandlerMap;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        InsuranceClaim costumer = new InsuranceClaim(
                "B47",
                "Dudung Su Dudung",
                "13000",
                "health"
        );
        String noKtp = "123456765432";

        var claimHandler = claimHandlerMap.get(costumer.getType());
        claimHandler.processClaim(costumer, noKtp);

        Customer newCost = new Customer("Dudung Su Dudung", "123456765432", 26, "health");

        claimHandler.addCustomer(newCost);
        claimHandler.processClaim(costumer, noKtp);
    }
}
