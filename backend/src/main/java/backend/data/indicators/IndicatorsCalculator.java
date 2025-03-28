package backend.data.indicators;

import java.util.List;

public class IndicatorsCalculator implements IIndicators{

    public double simpleMovingAverage(List<Double> asset,Integer period){
        if (asset == null || asset.isEmpty() || period <= 0) {
            throw new IllegalArgumentException("Invalid parameters: asset size must equal period and both must be positive");
        }
        if (asset.size() < period) {
            throw new IllegalArgumentException("Not enough data points: asset size must be at least equal to period");
        }
        List<Double> lastElements = asset.subList(Math.max(0, asset.size() - period), asset.size());
    return  lastElements.stream().mapToDouble(Double::doubleValue).sum()/period;
    }


    @Override
    public double exponentialMovingAverage(double currentValue, double yesterdayEMA, Integer smoothingFactor, Integer period) {
        if (smoothingFactor == null || period == null || period <= 0) {
            throw new IllegalArgumentException("Invalid parameters");
        }

        double multiplier = smoothingFactor.doubleValue() / (1.0 + period.doubleValue());
        return (currentValue * multiplier) + (yesterdayEMA * (1.0 - multiplier));
    }


    @Override
    public double relativeStrengthIndex(double relativeStrength) {
        if (relativeStrength == -1) {
            throw new IllegalArgumentException("relativeStrength cannot be -1 (would cause division by zero)");
        }
        return 100 - (100 / (1 + relativeStrength));
    }



    @Override
    public double movingAverageConvergenceDivergence(double ema12,double ema26) {
        return ema12 - ema26;
    }


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


    @Override
    public BollingerBands calculateBollingerBands(List<Double> asset, Integer period) {
        double sma = simpleMovingAverage(asset, period);
        double sd = calculateStandardDeviation(asset, period);
        return new BollingerBands(sma, sd);
    }


    @Override
    public double stochasticOscillatorK(double lastClosingPrice, double lowestPrice, double highestPrice) {
        if (highestPrice - lowestPrice == 0) {
            return 50;
        }
        return (lastClosingPrice - lowestPrice) / (highestPrice - lowestPrice) * 100;
    }
    @Override
    public double stochasticOscillatorD(List<Double> kValues) {
        return simpleMovingAverage(kValues, 3);
    }

    @Override
    public double onBalanceVolume(double previousOBV, double currentDayVolume, double lastClosingPrice, double currentClosingPrice)
    {
        if (currentDayVolume < 0) {
            throw new IllegalArgumentException("Volume cannot be negative");
        }
        if(currentClosingPrice > lastClosingPrice){
            return previousOBV+currentDayVolume;
        }else if(currentClosingPrice == lastClosingPrice){
            return previousOBV;
        }else{
            return previousOBV-currentDayVolume;
        }
    }
}
