package com.netflix.conductor.contribs;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import com.google.inject.multibindings.StringMapKey;
import com.google.inject.name.Named;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.pubsub.GooglePubSubQueueProvider;

import static com.netflix.conductor.core.events.EventQueues.EVENT_QUEUE_PROVIDERS_QUALIFIER;

/**
 * @author ajedro <github.com/jakon89>
 */
public class GooglePubSubModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @ProvidesIntoMap
    @StringMapKey("googlePubSub")
    @Singleton
    @Named(EVENT_QUEUE_PROVIDERS_QUALIFIER)
    public EventQueueProvider getGooglePubSubModule(Configuration configuration) {
        return new GooglePubSubQueueProvider(configuration);
    }
}
