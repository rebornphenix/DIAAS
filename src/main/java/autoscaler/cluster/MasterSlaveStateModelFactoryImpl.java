package autoscaler.cluster;

import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class MasterSlaveStateModelFactoryImpl extends StateModelFactory {
    private MasterSlaveStateModelFactory modelFactory;

    public MasterSlaveStateModelFactoryImpl(MasterSlaveStateModelFactory modelFactory) {
        this.modelFactory = modelFactory;
    }

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
        return new MasterSlaveStateModelImpl(modelFactory.createNewStateModel(resourceName, partitionName));
    }
}
