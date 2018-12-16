package com.netflix.conductor.contribs.queue.pubsub;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author ajedro <github.com/jakon89>
 */
class PubsubTestUtils {

    private final String projectId;
    private final FixedCredentialsProvider credentialsProvider;
    private final String topic;
    private final String subscriptionName;

    PubsubTestUtils(String projectId, String serviceKeyPath) {
        this.projectId = projectId;
        //google requires that any resource starts with letter
        this.topic = "t"+UUID.randomUUID().toString();
        this.subscriptionName = "s"+UUID.randomUUID().toString();
        try {
            this.credentialsProvider = FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(serviceKeyPath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void createTopic() throws Exception {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings())) {
            ProjectTopicName topic = ProjectTopicName.of(this.projectId, this.topic);
            topicAdminClient.createTopic(topic);
        }
    }

    void publishMessage(String message) throws Exception {
        BatchingSettings batchingSettings = BatchingSettings.newBuilder()
                .setIsEnabled(false)
                .build();

        Publisher publisher = Publisher
                .newBuilder(ProjectTopicName.of(this.projectId, this.topic))
                .setBatchingSettings(batchingSettings)
                .setCredentialsProvider(this.credentialsProvider)
                .build();

        publisher.publish(PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).build()).get();
        publisher.shutdown();
    }

    String createSubscription() throws Exception {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionSettings())) {
            Subscription subscription = Subscription.newBuilder()
                    .setTopic(ProjectTopicName.format(this.projectId, this.topic))
                    .setName(ProjectSubscriptionName.format(this.projectId, this.subscriptionName))
                    .build();
            subscriptionAdminClient.createSubscription(subscription);
            return subscription.getName();
        }
    }

    void clearEnv() throws Exception {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings())) {
            ProjectTopicName topic = ProjectTopicName.of(this.projectId, this.topic);
            topicAdminClient.deleteTopic(topic);
        }
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionSettings())) {
            subscriptionAdminClient.deleteSubscription(ProjectSubscriptionName.of(this.projectId, this.subscriptionName));
        }
    }

    private SubscriptionAdminSettings subscriptionSettings() throws IOException {
        return SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(this.credentialsProvider)
                .build();
    }

    private TopicAdminSettings topicAdminSettings() throws IOException {
        return TopicAdminSettings
                .newBuilder()
                .setCredentialsProvider(this.credentialsProvider)
                .build();
    }

    String getSubscription() {
        return subscriptionName;
    }
}
