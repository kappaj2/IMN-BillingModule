package za.co.tman.billing.service.messaging.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import za.co.tman.billing.enums.EventType;
import za.co.tman.billing.enums.PubSubMessageType;
import za.co.tman.billing.service.messaging.IMMessageProcessor;
import za.co.tman.billing.service.messaging.InterModulePubSubMessage;


@Component(value="imMessageProcessor")
public class IMMessageProcessorImpl implements IMMessageProcessor {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Override
    public void processMessageReceived(Message<?> message) {
        PubSubMessageType messageType = PubSubMessageType.GENERIC;
    
        String messageId = null;
        if(message.getHeaders().containsKey("id")){
            messageId = message.getHeaders().get("id").toString();
        }
        if (message.getHeaders().containsKey("PubSubMessageType")) {
            String mes = message.getHeaders().get("PubSubMessageType").toString();
            messageType = PubSubMessageType.findPubSubMessageType(mes);
        }
        log.info("MessageId : "+messageId);
    
        
        String payload = "";
    
        switch (messageType) {
            case GENERIC:
                try {
                    try {
                        log.info("Generic message received ...");
                        // sendTestMessage();
                        
                        
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                
                    payload = objectMapper.readValue(message.getPayload().toString(), String.class);
                } catch (IOException ioe) {
                    log.error("Error parsing payload : ", ioe.getMessage());
                }
                break;
            case INCIDENT:
                try {
                
                    payload = message.getPayload().toString();
                
                    InterModulePubSubMessage inboundMessage = objectMapper
                        .readValue(message.getPayload().toString(), InterModulePubSubMessage.class);
                
                    EventType eventType = inboundMessage.getEventType();
                    System.out.println(eventType.toString());
                    
                
                } catch (IOException io) {
                    io.printStackTrace();
                }
                break;
            default:
                payload = "Unknown message format received : ";
        }
    log.info("Payload   => "+payload);

    }
}
