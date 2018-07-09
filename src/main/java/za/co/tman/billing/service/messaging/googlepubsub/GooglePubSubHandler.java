package za.co.tman.billing.service.messaging.googlepubsub;

import za.co.tman.billing.service.messaging.InterModulePubSubMessage;


public interface GooglePubSubHandler {

    void subscribeToSubscription(String subscriptionName);
    void publishMessage(InterModulePubSubMessage interModulePubSubMessage, String topicName);
}
