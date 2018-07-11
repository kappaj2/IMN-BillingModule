package za.co.tman.billing.service.messaging.googlepubsub.impl;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import za.co.tman.billing.config.PubSubMessagingProperties;
import za.co.tman.billing.enums.PubSubMessageType;
import za.co.tman.billing.service.messaging.IMMessageProcessor;
import za.co.tman.billing.service.messaging.googlepubsub.GooglePubSubHandler;

import static junit.framework.TestCase.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest()
public class GooglePubSubHandlerImplTest {
    
    @Value("${spring.application.name}")
    private String applicationModuleName;
    
    private Fixture fixture;
    
    @MockBean
    private PubSubTemplate pubSubTemplateMock;
    
    @MockBean
    private IMMessageProcessor imMessageProcessor;
    
    @MockBean
    private ObjectMapper objectMapper;
    
    @Autowired
    private PubSubMessagingProperties pubSubMessagingProperties;
    
    @Before
    public void setUp() throws Exception {
        fixture = new Fixture();
    }
    
    @Test
    public void getTargetTopicNames() {
        
        PubSubMessageType mesType = PubSubMessageType.GENERIC;
        
        List<String> targetTopicNamesList = Arrays.asList("GenericTopic");
        
        fixture.givenThatPubSubMessageType(mesType);
        fixture.whenGetTargetTopicNames();
        fixture.thenTopicsNamesListShouldContain(targetTopicNamesList);
    }
    
    @Test
    public void getSubscriptions() {
        
        List<String> targetSubscriptionNamesList = Arrays.asList("BillingTopicSub", "BillingGenericSub");
        
        fixture.whenGetSubscriptions();
        fixture.thenSubscriptionNamesShouldContain(targetSubscriptionNamesList);
    }
    
    private class Fixture {
        
        private GooglePubSubHandler googlePubSubHandler1;
        private PubSubMessageType pubSubMessageType;
        private List<String> topicNamesList;
        private List<String> supscriptionNamesList;
        
        public Fixture() {
            MockitoAnnotations.initMocks(this);
            googlePubSubHandler1 = new GooglePubSubHandlerImpl(pubSubTemplateMock,
                imMessageProcessor,
                objectMapper,
                pubSubMessagingProperties,
                applicationModuleName);
            
        }
        
        public void givenThatPubSubMessageType(PubSubMessageType pubSubMessageType) {
            this.pubSubMessageType = pubSubMessageType;
        }
        
        public void whenGetTargetTopicNames() {
            topicNamesList = googlePubSubHandler1.getTargetTopicNames(pubSubMessageType);
        }
        
        public void whenGetSubscriptions() {
            supscriptionNamesList = googlePubSubHandler1.getSubscriptionsForModule();
        }
        
        public void thenTopicsNamesListShouldContain(List<String> targetNames) {
            for (String targetName : targetNames) {
                assertTrue(topicNamesList.contains(targetName));
            }
        }
        
        public void thenSubscriptionNamesShouldContain(List<String> targetSubscriptionNamesList) {
            
            for (String subscriptionName : targetSubscriptionNamesList) {
                assertTrue(targetSubscriptionNamesList.contains(subscriptionName));
            }
        }
    }
}
