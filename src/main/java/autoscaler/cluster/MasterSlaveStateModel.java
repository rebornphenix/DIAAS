package autoscaler.cluster;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;


public interface MasterSlaveStateModel {
    void onBecomeMasterFromOffline (Message message, NotificationContext context);
    void onBecomeSlaveFromOffline (Message message, NotificationContext context);
    void onBecomeMasterFromSlave (Message message, NotificationContext context);
    void onBecomeSlaveFromMaster (Message message, NotificationContext context);
    void onBecomeOfflineFromMaster (Message message, NotificationContext context);
    void onBecomeOfflineFromSlave (Message message, NotificationContext context);
    void onBecomeDroppedFromOffline (Message message, NotificationContext context);
    void onBecomeDroppedFromError (Message message, NotificationContext context);
}
