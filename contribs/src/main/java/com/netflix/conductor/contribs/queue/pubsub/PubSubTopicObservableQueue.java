package com.netflix.conductor.contribs.queue.pubsub;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import rx.Observable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author ajedro <github.com/jakon89>
 */
public class PubSubTopicObservableQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(PubSubTopicObservableQueue.class);

    private Publisher publisher;
    private final String queueURI;
    private final long elementCountThreshod;
    private final int delayThresholdSeconds;
    private final GoogleCredentials credentials;

    public PubSubTopicObservableQueue(
            String queueURI,
            long elementCountThreshod,
            int delayThresholdSeconds,
            GoogleCredentials credentials
    ) {
        this.queueURI = queueURI;
        this.elementCountThreshod = elementCountThreshod;
        this.delayThresholdSeconds = delayThresholdSeconds;
        this.credentials = credentials;
    }

    @Override
    public void publish(List<Message> messages) {
        initPublisher();

        messages
                .stream()
                .map(e -> PubsubMessage
                        .newBuilder()
                        .setData(ByteString.copyFromUtf8(e.getPayload()))
                        .setMessageId(e.getId()).build())
                .forEach(publisher::publish);
    }

    private synchronized void initPublisher() {
        if (publisher != null) {
            return;
        }

        BatchingSettings batchingSettings = BatchingSettings.newBuilder()
                .setElementCountThreshold(elementCountThreshod)
                .setDelayThreshold(Duration.ofSeconds(delayThresholdSeconds))
                .build();

        Duration retryDelay = Duration.ofMillis(500);
        double retryDelayMultiplier = 2.0;
        Duration maxRetryDelay = Duration.ofSeconds(10);

        RetrySettings retrySettings = RetrySettings.newBuilder()
                .setInitialRetryDelay(retryDelay)
                .setRetryDelayMultiplier(retryDelayMultiplier)
                .setMaxRetryDelay(maxRetryDelay)
                .setMaxRpcTimeout(Duration.ofSeconds(60))
                .setRpcTimeoutMultiplier(2)
                .setInitialRpcTimeout(Duration.ofSeconds(5))
                .setTotalTimeout(Duration.ofSeconds(120))
                .build();

        try {
            this.publisher = Publisher
                    .newBuilder(getTopicName())
                    .setBatchingSettings(batchingSettings)
                    .setRetrySettings(retrySettings)
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build();
        } catch (IOException e) {
            logger.error("Cannot create publisher for topic {} {}", getTopicName(), e);
            throw new RuntimeException("Cannot creat Google Pub/Sub publisher", e);
        }
    }

    @Override
    public Observable<Message> observe() {
        return Observable.empty();
    }

    @Override
    public String getType() {
        return "googlePubSub";
    }

    @Override
    public String getName() {
        return queueURI;
    }

    @Override
    public String getURI() {
        return queueURI;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        return Collections.emptyList();
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {

    }

    @Override
    public long size() {
        return 0;
    }

    public String getQueueURI() {
        return queueURI;
    }

    public long getElementCountThreshod() {
        return elementCountThreshod;
    }

    public int getDelayThresholdSeconds() {
        return delayThresholdSeconds;
    }

    private String getTopicName() {
        int index = this.queueURI.indexOf(':');
        if (index == -1) {
            logger.error("Topic name cannot be derived for illegal queueURI: {}", this.queueURI);
            throw new IllegalArgumentException("Illegal queueURI " + this.queueURI);
        }

        String projectId = this.queueURI.substring(0, index);
        String topicId = this.queueURI.substring(index + 1);

        if (StringUtils.isEmpty(projectId) || StringUtils.isEmpty(topicId)) {
            logger.error("Project id [{}] or topic id [{}] is empty", projectId, topicId);
            throw new IllegalArgumentException("Illegal project id or topic id");
        }

        return ProjectTopicName.format(projectId, topicId);
    }
}
