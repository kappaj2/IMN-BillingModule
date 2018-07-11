package za.co.tman.billing.service.messaging.googlepubsub;

import java.util.List;

import za.co.tman.billing.enums.PubSubMessageType;
import za.co.tman.billing.service.messaging.InterModulePubSubMessage;


public interface GooglePubSubHandler {

    void subscribeToSubscription(String subscriptionName);
    void publishMessage(InterModulePubSubMessage interModulePubSubMessage);
    List<String> getTargetTopicNames(PubSubMessageType pubSubMessageType);
    List<String> getSubscriptionsForModule();
}
