package backend.data.indicators;

import java.util.List;

public interface IIndicators {
    public double simpleMovingAverage(List<Double> asset,Integer period);

    double exponentialMovingAverage(double currentValue, double yesterdayEMA, Integer smoothingFactor, Integer period);

    double relativeStrengthIndex(double relativeStrength);

    double movingAverageConvergenceDivergence(double ema12, double ema26);

    BollingerBands calculateBollingerBands(List<Double> asset, Integer period);

    double stochasticOscillatorK(double lastClosingPrice, double lowestPrice, double highestPrice);

    double stochasticOscillatorD(List<Double> kValues);

    double onBalanceVolume(double previousOVB, double currentDayVolume, double lastClosingPrice, double currentClosingPrice);
}
