package com.netflix.conductor.contribs.queue.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.netflix.conductor.core.events.queue.Message;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.observables.BlockingObservable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.junit.Assert.*;

/**
 * @author ajedro <github.com/jakon89>
 */
public class PubSubSubscriptionObservableQueueTest {

    private String projectId;
    private String keyPath;
    private PubsubTestUtils pubsubTestUtils;

    @Before
    public void setup() throws Exception {
        this.projectId = System.getenv("TEST_GOOGLE_PROJECT_ID");
        this.keyPath = System.getenv("TEST_GOOGLE_APPLICATION_CREDENTIALS");

        //run test only if above ENVs are specified
        Assume.assumeFalse(isBlank(projectId) || isBlank(keyPath));

        pubsubTestUtils = new PubsubTestUtils(projectId, keyPath);

        pubsubTestUtils.createTopic();
        pubsubTestUtils.createSubscription();
    }

    @After
    public void after() throws Exception {
        pubsubTestUtils.clearEnv();
    }

    @Test
    public void shouldReceiveMessageWithCredentialsConfiguredByKeyPath() throws Exception {
        pubsubTestUtils.publishMessage("message");

        PubSubSubscriptionObservableQueue pubSubSubscriptionObservableQueue = new PubSubSubscriptionObservableQueue(
                projectId + ":" + pubsubTestUtils.getSubscription(),
                60,
                10,
                500,
                credentials()
        );

        Observable<Message> observe = pubSubSubscriptionObservableQueue.observe();
        Message first = BlockingObservable.from(observe).first();

        assertEquals("message", first.getPayload());
    }

    @Test
    public void shouldReceiveMessageWithCredentialsConfiguredByDefault() throws Exception {
        //run only if GOOGLE_APPLICATION_CREDENTIALS is specified
        //GOOGLE_APPLICATION_CREDENTIALS is used by Google Pub/Sub library to derive service account key
        Assume.assumeFalse(isBlank(System.getenv("GOOGLE_APPLICATION_CREDENTIALS")));

        String message = "messageX";
        pubsubTestUtils.publishMessage(message);

        PubSubSubscriptionObservableQueue pubSubSubscriptionObservableQueue = new PubSubSubscriptionObservableQueue(
                projectId + ":" + pubsubTestUtils.getSubscription(),
                60,
                10,
                500,
                credentials()
        );

        Observable<Message> observe = pubSubSubscriptionObservableQueue.observe();
        Message first = BlockingObservable.from(observe).first();

        assertEquals(message, first.getPayload());
    }

    //This test is flaky as Pub/Sub might deliver message twice.
    //It's rare case though.
    @Test
    public void shouldReceiveMessageWithBatchSizeSpecified() throws Exception {
        pubsubTestUtils.publishMessage("message1");
        pubsubTestUtils.publishMessage("message2");
        pubsubTestUtils.publishMessage("message3");

        PubSubSubscriptionObservableQueue pubSubSubscriptionObservableQueue = new PubSubSubscriptionObservableQueue(
                projectId + ":" + pubsubTestUtils.getSubscription(),
                60,
                2,
                500,
                credentials()
        );

        List<Message> firstBatch = pubSubSubscriptionObservableQueue.receiveMessages();
        assertEquals(2, firstBatch.size());

        List<Message> secondBatch = pubSubSubscriptionObservableQueue.receiveMessages();
        assertEquals(1, secondBatch.size());

        HashSet<String> receivedMessages = new HashSet<>();
        receivedMessages.addAll(firstBatch.stream().map(Message::getPayload).collect(toList()));
        receivedMessages.addAll(secondBatch.stream().map(Message::getPayload).collect(toList()));

        assertTrue(receivedMessages.contains("message1"));
        assertTrue(receivedMessages.contains("message2"));
        assertTrue(receivedMessages.contains("message3"));
    }

    @Test
    public void shouldReceiveMessageTwiceIfNonAcked() throws Exception {
        pubsubTestUtils.publishMessage("message");

        PubSubSubscriptionObservableQueue pubSubSubscriptionObservableQueue = new PubSubSubscriptionObservableQueue(
                projectId + ":" + pubsubTestUtils.getSubscription(),
                1,
                2,
                500,
                credentials()
        );

        Observable<Message> observe = pubSubSubscriptionObservableQueue.observe();
        BlockingObservable<Message> blockingObservable = BlockingObservable.from(observe);

        assertEquals("message", blockingObservable.first().getPayload());

        //second message should arrive after ackTimeoutInSec + pollTimeInMS
        assertEquals("message", blockingObservable.first().getPayload());
    }

    @Test
    public void shouldNotReceiveSecondMessageTwiceIfAcked() throws Exception {
        pubsubTestUtils.publishMessage("message");

        PubSubSubscriptionObservableQueue pubSubSubscriptionObservableQueue = new PubSubSubscriptionObservableQueue(
                projectId + ":" + pubsubTestUtils.getSubscription(),
                15,
                2,
                500,
                credentials()
        );

        Observable<Message> observe = pubSubSubscriptionObservableQueue.observe();
        BlockingObservable<Message> blockingObservable = BlockingObservable.from(observe);

        Message first = blockingObservable.first();
        assertEquals("message", first.getPayload());

        pubSubSubscriptionObservableQueue.ack(Collections.singletonList(first));

        //should throw TimeoutException as first message was acked
        try {
            observe.timeout(15, TimeUnit.SECONDS).toBlocking().first();
        } catch (RuntimeException e) {

            //unwrap cause and check that it is TimeoutException
            Throwable cause = e.getCause();
            while (cause.getCause() != null) cause = e.getCause();

            if (!(cause instanceof TimeoutException)) {
                fail("Cause is not TimeoutException but " + cause);
            }
        }
    }

    private GoogleCredentials credentials() {
        GoogleCredentials credentials;
        try {
            if (StringUtils.isEmpty(keyPath)) {
                credentials = GoogleCredentials.getApplicationDefault();
            } else {
                credentials = GoogleCredentials.fromStream(new FileInputStream(keyPath));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return credentials;
    }
}