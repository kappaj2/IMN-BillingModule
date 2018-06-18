package za.co.tman.billing.service.messaging;

import org.springframework.messaging.Message;


public interface IMMessageProcessor {
    
    void processMessageReceived(Message<?> message);

}
