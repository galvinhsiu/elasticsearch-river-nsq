package org.elasticsearch.plugin.river.nsq;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.nsq.NsqRiverModule;

/**
 *
 */
public class NsqRiverPlugin extends AbstractPlugin {

    @Inject
    public NsqRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-nsq";
    }

    @Override
    public String description() {
        return "River NSQ Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("nsq", NsqRiverModule.class);
    }
}
