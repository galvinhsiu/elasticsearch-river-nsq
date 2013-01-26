package org.elasticsearch.river.nsq;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 *
 */
public class NsqRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(NsqBatchRiver.class).asEagerSingleton();
    }
}
