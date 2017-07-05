package kafka.server;

public enum BrokerState {

    NotRunning((byte) 0), //
    Starting((byte) 1), //
    RecoveringFromUncleanShutdown((byte) 2), //
    RunningAsBroker((byte) 3), ///
    RunningAsController((byte) 4), //
    PendingControlledShutdown((byte) 6), //
    BrokerShuttingDown((byte) 7);

    private Byte state;

    private BrokerState(Byte state) {
        this.state = state;
    }

    private BrokerState(BrokerState brokerState) {
        this(brokerState.state);
    }

    public Byte getState() {
        return state;
    }
}
