package backend.data.indicators;

import java.util.List;

public class IndicatorsCalulator implements IIndicators{
    /**
     *
     * @param asset
     * @param period
     * @return
     */
    public double simpleMovingAverage(List<Double> asset,Integer period){
        if (asset == null || asset.isEmpty() || period <= 0 || asset.size() != period) {
            throw new IllegalArgumentException("Invalid parameters: asset size must equal period and both must be positive");
        }
    return  asset.stream().mapToDouble(Double::doubleValue).sum()/period;
    }

    /**
     *
     * @param currentValue
     * @param yesterdayEMA
     * @param smoothingFactor
     * @param period
     * @return
     */
    @Override
    public double exponentialMovingAverage(double currentValue, double yesterdayEMA, Integer smoothingFactor, Integer period) {
        if (smoothingFactor == null || period == null || period <= 0) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        double multiplier = smoothingFactor.doubleValue() / (1.0 + period.doubleValue());
        return (currentValue * multiplier) + (yesterdayEMA * (1.0 - multiplier));
    }

    /**
     *
     * @param relativeStrength
     * @return
     */
    @Override
    public double relativeStrengthIndex(double relativeStrength) {
        return 100-(100/(1+relativeStrength));
    }

    /**
     *
     * @param ema12
     * @param ema26
     * @return
     */
    @Override
    public double movingAverageConvergenceDivergence(double ema12,double ema26) {
        return ema12 - ema26;
    }

    /**
     *
     * @param asset
     * @param period
     * @return
     */
    private double calculateStandardDeviation(List<Double> asset, Integer period) {
        if (asset == null || asset.isEmpty() || period <= 0 || period > asset.size()) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        double sum = 0;
        double sma = simpleMovingAverage(asset, period);

        // Get the last 'period' elements
        List<Double> lastElements = asset.subList(Math.max(0, asset.size() - period), asset.size());

        for (Double value : lastElements) {
            sum += Math.pow(value - sma, 2);
        }

        return Math.sqrt(sum / period);
    }

    /**
     *
     * @param asset
     * @param period
     * @return
     */
    @Override
    public BollingerBands calculateBollingerBands(List<Double> asset, Integer period) {
        double sma = simpleMovingAverage(asset, period);
        double sd = calculateStandardDeviation(asset, period);
        return new BollingerBands(sma, sd);
    }

    /**
     *
     * @return
     */
    @Override
    public double stochasticOcillator(double recentClosingPrice,) {

    }

    /**
     *
     * @return
     */
    @Override
    public double onBalanceVolume() {
        return 0;
    }
}
