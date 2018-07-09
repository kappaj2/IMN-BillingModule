package za.co.tman.billing.service.messaging;

public interface IMMessageProcessor {
    
    void processMessageReceived(InterModulePubSubMessage interModulePubSubMessage);

}
