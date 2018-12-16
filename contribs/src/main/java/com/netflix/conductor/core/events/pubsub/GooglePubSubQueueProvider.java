package com.netflix.conductor.core.events.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.netflix.conductor.contribs.queue.pubsub.PubSubSubscriptionObservableQueue;
import com.netflix.conductor.contribs.queue.pubsub.PubSubTopicObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ajedro <github.com/jakon89>
 */
@Singleton
public class GooglePubSubQueueProvider implements EventQueueProvider {
    private static Logger logger = LoggerFactory.getLogger(GooglePubSubQueueProvider.class);

    private final Map<String, ObservableQueue> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, ObservableQueue> topics = new ConcurrentHashMap<>();

    private final int batchSize;
    private final int pollTimeInMS;
    private final int ackTimeoutInSeconds;
    private final GoogleCredentials credentials;
    private final long elementCountThreshold;
    private final int delayThresholdSeconds;

    @Inject
    public GooglePubSubQueueProvider(Configuration config) {
        this.batchSize = config.getIntProperty("workflow.event.queues.googlePubSub.batchSize", 1);
        this.pollTimeInMS = config.getIntProperty("workflow.event.queues.googlePubSub.pollTimeInMS", 100);
        this.ackTimeoutInSeconds = config.getIntProperty("workflow.event.queues.googlePubSub.ackTimeoutInSeconds", 60);
        String serviceKeyPath = config.getProperty("workflow.event.queues.googlePubSub.serviceKeyPath", "");
        this.elementCountThreshold = config.getLongProperty("workflow.event.queues.googlePubSub.elementCountThreshold", 100);
        this.delayThresholdSeconds = config.getIntProperty("workflow.event.queues.googlePubSub.delayThresholdSeconds", 3);
        this.credentials = readCredentials(serviceKeyPath);
    }

    /*
    Google Pub/Sub isn't typical queue.
    There are topics (where you push messages) and subscriptions (where you listen for messages).
    This means we need custom format of 'sink' property used across conductor so we are able to distinguish between publish and subscribe actions.
     */
    @Override
    public ObservableQueue getQueue(String queueURI) {
        if (isTopic(queueURI)) {
            return topics.computeIfAbsent(queueURI, q -> {
                return new PubSubTopicObservableQueue(
                        queueURI.replace("topic:", ""),
                        elementCountThreshold,
                        delayThresholdSeconds,
                        credentials
                );
            });
        }

        return subscriptions.computeIfAbsent(queueURI, q -> {
            return new PubSubSubscriptionObservableQueue(
                    queueURI.replace("subscription:", ""),
                    ackTimeoutInSeconds,
                    batchSize,
                    pollTimeInMS,
                    credentials
            );
        });
    }

    private boolean isTopic(String queueURI) {
        //formats used in conductor
        //googlePubSub:subscription:my-awesome-project:my-subscription - subscription
        //googlePubSub:topic:my-awesome-project:my-awesome-topic - topic
        return queueURI.startsWith("topic");
    }


    GoogleCredentials readCredentials(String serviceKeyPath) {
        try {
            if (StringUtils.isEmpty(serviceKeyPath)) {
                return GoogleCredentials.getApplicationDefault();
            } else {
                return GoogleCredentials.fromStream(new FileInputStream(serviceKeyPath));
            }
        } catch (IOException e) {
            logger.error("Exception during obtaining GoogleCredentials");
            throw new RuntimeException(e);
        }
    }
}
