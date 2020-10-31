package lab3;

public class FlightStatistics {
    String percentCancelled;
    String percentDelay;
    String timeDelay;
    String cancelled;
    String delay;

    public String getCancelled() {
        return cancelled;
    }

    public String getDelay() {
        return delay;
    }

    public FlightStatistics(String delay, String cancelled) {
        this.cancelled = cancelled;
        this.delay = delay;
    }

    public String getPercentCancelled() {
        return percentCancelled;
    }

    public String getPercentDelay() {
        return percentDelay;
    }

    public String getTimeDelay() {
        return timeDelay;
    }
}
