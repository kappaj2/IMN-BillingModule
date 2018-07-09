package za.co.tman.billing.service.messaging.googlepubsub.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import za.co.tman.billing.config.MessageImplementationCondition;
import za.co.tman.billing.config.PubSubMessagingProperties;
import za.co.tman.billing.enums.PubSubMessageType;
import za.co.tman.billing.service.messaging.IMMessageProcessor;
import za.co.tman.billing.service.messaging.InterModulePubSubMessage;
import za.co.tman.billing.service.messaging.googlepubsub.GooglePubSubHandler;


@Component
@Configuration
@Conditional(MessageImplementationCondition.class)
public class GooglePubSubHandlerImpl implements GooglePubSubHandler {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    @Value("${spring.application.name}")
    private String applicationModuleName;
    
    private PubSubTemplate pubSubTemplate;
    private PubSubAdmin pubSubAdmin;
    private IMMessageProcessor imMessageProcessor;
    private ObjectMapper objectMapper;
    private PubSubMessagingProperties pubSubMessagingProperties;
    
    public GooglePubSubHandlerImpl(PubSubTemplate pubSubTemplate,
                                   PubSubAdmin pubSubAdmin,
                                   IMMessageProcessor imMessageProcessor,
                                   ObjectMapper objectMapper,
                                   PubSubMessagingProperties pubSubMessagingProperties) {
        this.pubSubTemplate = pubSubTemplate;
        this.pubSubAdmin = pubSubAdmin;
        this.imMessageProcessor = imMessageProcessor;
        this.objectMapper = objectMapper;
        this.pubSubMessagingProperties = pubSubMessagingProperties;
        
    }
    
    @PostConstruct
    private void setupConnection() {
        subscribeToSubscription("BillingGenericSub");
    }
    
    @Override
    public void subscribeToSubscription(String subscriptionName) {
        
        pubSubTemplate.subscribe(subscriptionName, (pubsubMessage, ackReplyConsumer) -> {
            
            try {
                String messageData = pubsubMessage.getData().toStringUtf8();
                Map<String, String> attributesMap = pubsubMessage.getAttributesMap();
                String messageType = attributesMap.get("MessageType");
                
                if (messageType == null || messageType.equals("String")) {
                    
                    log.info("Received normal string message : " + messageData);
                    
                } else {
                    
                    InterModulePubSubMessage interModulePubSubMessage = objectMapper
                        .readValue(messageData, InterModulePubSubMessage.class);
                    interModulePubSubMessage.setMessageHeaders(attributesMap);
                    interModulePubSubMessage.setMessageId(pubsubMessage.getMessageId());
                    
                    imMessageProcessor.processMessageReceived(interModulePubSubMessage);
    
                    publishMessage(interModulePubSubMessage, "GenericTopic");
                }
                
            } catch (Exception ex) {
                log.error("Error decoding received message", ex);
            }
            ackReplyConsumer.ack();
        });
    }
    
    @Override
    public void publishMessage(InterModulePubSubMessage interModulePubSubMessage, String topicName) {

        try {
            Publisher publisher = Publisher.newBuilder(topicName).build();
    
            String payloadJson = objectMapper.writeValueAsString(interModulePubSubMessage);
    
            PubsubMessage pubsubMessage =
                PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(payloadJson))
                    .putAttributes("MessageType", interModulePubSubMessage.getPubSubMessageType().getMessageTypeCode())
                    .build();
            
            publisher.publish(pubsubMessage);
            
        }catch(IOException io){
            log.error("Error processing submit : "+io.getLocalizedMessage(), io);
        }
    }
    
    /**
     * Retrieve the list of topics the message must be send to.
     *
     * @param pubSubMessageType
     * @return List<String> A list of topic names. If none found then the list will be empty.
     */
    private List<String> getTargetTopicNames(PubSubMessageType pubSubMessageType) {
        
        List<PubSubMessagingProperties.Modules> modules
            = pubSubMessagingProperties.getModules().stream().filter(module ->
            (module.getApplicationModuleName().equals(applicationModuleName)
                && module.getPubSubMessageType().equalsIgnoreCase(pubSubMessageType.getMessageTypeCode())))
            .collect(Collectors.toList());
        
        if (!modules.isEmpty()) {
            return modules.get(0).getTopicsList();
        } else {
            return new ArrayList<>();
        }
        
    }
}
