package backend.infrastructure.spark.indicators;

public class BollingerBands {
    private double upperBand;
    private double middleBand;
    private double lowerBand;

    public BollingerBands(double SMA20, double SD20) {
        this.upperBand = SMA20 + (2 * SD20);
        this.middleBand = SMA20;
        this.lowerBand = SMA20 - (2 * SD20);
    }

    // Add getters to access the bands
    public double getUpperBand() {
        return upperBand;
    }

    public double getMiddleBand() {
        return middleBand;
    }

    public double getLowerBand() {
        return lowerBand;
    }
}
