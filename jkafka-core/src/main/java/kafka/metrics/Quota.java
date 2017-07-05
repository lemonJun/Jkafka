package kafka.metrics;

public class Quota {

    private final boolean upper;
    private final double bound;

    public Quota(double bound, boolean upper) {
        this.bound = bound;
        this.upper = upper;
    }

    public static Quota upperBound(double upperBound) {
        return new Quota(upperBound, true);
    }

    public static Quota lowerBound(double lowerBound) {
        return new Quota(lowerBound, false);
    }

    public boolean isUpperBound() {
        return this.upper;
    }

    public double bound() {
        return this.bound;
    }

    public boolean acceptable(double value) {
        return (upper && value <= bound) || (!upper && value >= bound);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) this.bound;
        result = prime * result + (this.upper ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof Quota))
            return false;
        Quota that = (Quota) obj;
        return (that.bound == this.bound) && (that.upper == this.upper);
    }
}
