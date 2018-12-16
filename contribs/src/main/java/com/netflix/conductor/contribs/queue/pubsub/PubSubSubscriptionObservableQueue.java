package com.netflix.conductor.contribs.queue.pubsub;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author ajedro <github.com/jakon89>
 */
public class PubSubSubscriptionObservableQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(PubSubSubscriptionObservableQueue.class);

    private static final String QUEUE_TYPE = "googlePubSub";

    private final int ackTimeoutInSeconds;
    private final int batchSize;
    private final int pollTimeInMS;
    private final String subscriptionName; // project-id:subscription-id
    private final String queueURI;
    private final GoogleCredentials credentials;

    public PubSubSubscriptionObservableQueue(
            String queueURI,
            int ackTimeoutInSeconds,
            int batchSize,
            int pollTimeInMS,
            GoogleCredentials googleCredentials)
    {
        this.queueURI = queueURI;
        this.subscriptionName = getSubscriptionNameInGoogleFormat();
        this.ackTimeoutInSeconds = ackTimeoutInSeconds;
        this.batchSize = batchSize;
        this.pollTimeInMS = pollTimeInMS;
        this.credentials = googleCredentials;
    }

    @Override
    public Observable<Message> observe() {
        Observable.OnSubscribe<Message> subscriber = getOnSubscribe();
        return Observable.create(subscriber);
    }

    @Override
    public String getType() {
        return QUEUE_TYPE;
    }

    @Override
    public String getName() {
        return queueURI;
    }

    @Override
    public String getURI() {
        return queueURI;
    }

    public int getPollTimeInMS() {
        return pollTimeInMS;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getAckTimeoutInSeconds() {
        return ackTimeoutInSeconds;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        List<String> ackIds = messages.stream()
                .map(Message::getReceipt)
                .collect(Collectors.toList());

        AcknowledgeRequest acknowledgeRequest =
                AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionName)
                        .addAllAckIds(ackIds)
                        .build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(getSubscriberSettings())) {
            subscriber.acknowledgeCallable().call(acknowledgeRequest);
        } catch (IOException e) {
            logger.error("Cannot ack messages due to", e);
            return Collections.emptyList();
        }

        return ackIds;
    }

    @Override
    public void publish(List<Message> messages) {
        logger.warn("Pub/Sub publish not implemented. Message cannot be sent to pubsub subscription");
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
        logger.warn("Pub/Sub setUnackTimeout not implemented");
    }

    @Override
    public long size() {
        //this is not supported in Google Pub/Sub
        return -1;
    }

    private Observable.OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            Observable<Long> interval = Observable.interval(pollTimeInMS, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x)->{
                List<Message> msgs = receiveMessages();
                return Observable.from(msgs);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
    }

    @VisibleForTesting
    List<Message> receiveMessages() {
            PullRequest pullRequest =
                    PullRequest.newBuilder()
                            .setMaxMessages(batchSize)
                            .setSubscription(subscriptionName)
                            .build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(getSubscriberSettings())) {
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

            List<Message> messages = pullResponse
                    .getReceivedMessagesList()
                    .stream()
                    .map(e -> {
                        return new Message(
                                e.getMessage().getMessageId(),
                                e.getMessage().getData().toStringUtf8(),
                                e.getAckId()
                        );
                    })
                    .collect(Collectors.toList());

            modifyAckDeadline(subscriber, pullResponse);

            Monitors.recordEventQueueMessagesProcessed(QUEUE_TYPE, this.queueURI, messages.size());
            return messages;
        } catch (IOException e) {
            Monitors.recordObservableQMessageReceivedErrors(QUEUE_TYPE);
            logger.error("Cannot pull messages due to", e);
            return Collections.emptyList();
        }
    }

    private void modifyAckDeadline(SubscriberStub subscriber, PullResponse pullResponse) {
        ModifyAckDeadlineRequest modifyAckDeadlineRequest = ModifyAckDeadlineRequest.newBuilder()
                .setSubscription(getSubscriptionNameInGoogleFormat())
                .setAckDeadlineSeconds(ackTimeoutInSeconds)
                .addAllAckIds(pullResponse.getReceivedMessagesList().stream().map(ReceivedMessage::getAckId).collect(Collectors.toList()))
                .build();
        subscriber.modifyAckDeadlineCallable().call(modifyAckDeadlineRequest);
    }

    private SubscriberStubSettings getSubscriberSettings() throws IOException {
        SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials))
                .setTransportChannelProvider(
                        SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                .build());
        return builder.build();
    }

    private String getSubscriptionNameInGoogleFormat() {
        int index = this.queueURI.indexOf(':');
        if (index == -1) {
            logger.error("Subscription name cannot be derived for illegal queueURI: {}", this.queueURI);
            throw new IllegalArgumentException("Illegal queueURI " + this.queueURI);
        }

        String projectId = this.queueURI.substring(0, index);
        String subscriptionId = this.queueURI.substring(index + 1);

        if (StringUtils.isEmpty(projectId) || StringUtils.isEmpty(subscriptionId)) {
            logger.error("Project id [{}] or subscription id [{}] is empty", projectId, subscriptionId);
            throw new IllegalArgumentException("Illegal project id or subscription id");
        }

        return ProjectSubscriptionName.format(projectId, subscriptionId);
    }
}
