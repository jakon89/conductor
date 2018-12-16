package com.netflix.conductor.core.events.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.netflix.conductor.contribs.queue.pubsub.PubSubSubscriptionObservableQueue;
import com.netflix.conductor.contribs.queue.pubsub.PubSubTopicObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author ajedro <github.com/jakon89>
 */
public class GooglePubSubQueueProviderUnitTest {

    private Configuration configuration;

    public static class Sut extends GooglePubSubQueueProvider {
        public Sut(Configuration config) {
            super(config);
        }

        @Override
        GoogleCredentials readCredentials(String serviceKeyPath) {
            return null;
        }
    }

    @Before
    public void setup() {
        configuration = mock(Configuration.class);
    }

    @Test
    public void testGetSubscriptionQueueWithCustomConfiguration() {
        when(configuration.getIntProperty(eq("workflow.event.queues.googlePubSub.batchSize"), anyInt())).thenReturn(10);
        when(configuration.getIntProperty(eq("workflow.event.queues.googlePubSub.pollTimeInMS"), anyInt())).thenReturn(50);
        when(configuration.getIntProperty(eq("workflow.event.queues.googlePubSub.ackTimeoutInSeconds"), anyInt())).thenReturn(30);

        GooglePubSubQueueProvider pubSubQueueProvider = new Sut(configuration);
        PubSubSubscriptionObservableQueue observableQueue = (PubSubSubscriptionObservableQueue)pubSubQueueProvider.getQueue("subscription:shop-project:orders-subscription");

        assertNotNull(pubSubQueueProvider);
        assertEquals(10, observableQueue.getBatchSize());
        assertEquals(50, observableQueue.getPollTimeInMS());
        assertEquals(30, observableQueue.getAckTimeoutInSeconds());
        assertEquals("projects/shop-project/subscriptions/orders-subscription", observableQueue.getSubscriptionName());
        assertEquals("shop-project:orders-subscription", observableQueue.getURI());
    }

    @Test
    public void testGetTopicQueueWithCustomConfiguration() {
        when(configuration.getLongProperty(eq("workflow.event.queues.googlePubSub.elementCountThreshold"), anyInt())).thenReturn(33l);
        when(configuration.getIntProperty(eq("workflow.event.queues.googlePubSub.delayThresholdSeconds"), anyInt())).thenReturn(22);

        GooglePubSubQueueProvider pubSubQueueProvider = new Sut(configuration);
        PubSubTopicObservableQueue pubSubTopicObservableQueue = (PubSubTopicObservableQueue)pubSubQueueProvider.getQueue("topic:shop-project:orders-topic");

        assertNotNull(pubSubQueueProvider);
        assertEquals(33, pubSubTopicObservableQueue.getElementCountThreshod());
        assertEquals(22, pubSubTopicObservableQueue.getDelayThresholdSeconds());
        assertEquals("shop-project:orders-topic", pubSubTopicObservableQueue.getURI());
    }
}