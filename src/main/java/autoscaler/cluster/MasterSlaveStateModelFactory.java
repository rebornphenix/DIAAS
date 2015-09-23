package autoscaler.cluster;


public interface MasterSlaveStateModelFactory {
    public MasterSlaveStateModel createNewStateModel(String resourceName, String partitionName);
}
