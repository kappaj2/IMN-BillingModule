package za.co.tman.billing.cucumber.stepdefs;

import za.co.tman.billing.BillingmoduleApp;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.ResultActions;

import org.springframework.boot.test.context.SpringBootTest;

@WebAppConfiguration
@SpringBootTest
@ContextConfiguration(classes = BillingmoduleApp.class)
public abstract class StepDefs {

    protected ResultActions actions;

}
