package autoscaler.cluster;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.Transition;

public class MasterSlaveStateModelImpl extends StateModel implements MasterSlaveStateModel {
    private final MasterSlaveStateModel masterSlaveStateModel;

    public MasterSlaveStateModelImpl(MasterSlaveStateModel masterSlaveStateModel) {
        this.masterSlaveStateModel = masterSlaveStateModel;
    }

    @Override
    public void onBecomeMasterFromOffline(Message message, NotificationContext context) {
        masterSlaveStateModel.onBecomeMasterFromOffline(message, context);
    }

    @Override
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
        masterSlaveStateModel.onBecomeSlaveFromOffline(message, context);
    }

    @Override
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
        masterSlaveStateModel.onBecomeMasterFromSlave(message, context);
    }

    @Override
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
        masterSlaveStateModel.onBecomeSlaveFromMaster(message, context);
    }

    @Override
    public void onBecomeOfflineFromMaster(Message message, NotificationContext context) {
        masterSlaveStateModel.onBecomeOfflineFromMaster(message, context);
    }

    @Override
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
        masterSlaveStateModel.onBecomeOfflineFromSlave(message, context);
    }

    @Override
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        masterSlaveStateModel.onBecomeDroppedFromOffline(message, context);
    }

    @Transition(
            to = "DROPPED",
            from = "ERROR"
    )
    @Override
    public void onBecomeDroppedFromError(Message message, NotificationContext context){
        masterSlaveStateModel.onBecomeDroppedFromError(message, context);
        try {
            super.onBecomeDroppedFromError(message, context);
        } catch (Exception e) {
        }
    }
}
