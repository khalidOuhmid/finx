package backend.data.indicators;

import java.util.List;

public interface IIndicators {
    public double simpleMovingAverage(List<Double> asset,Integer period);
    public double exponentialMovingAverage();

    double exponentialMovingAverage(double currentValue, double yesterdayEMA, Integer smoothingFactor, Integer period);

    public double relativeStrengthIndex();
    public double movingAverageConvergenceDivergence();

    double relativeStrengthIndex(double relativeStrength);

    double movingAverageConvergenceDivergence(double ema12, double ema26);

    public double BollingerBands();

    BollingerBands calculateBollingerBands(List<Double> asset, Integer period);

    public double stochasticOcillator();
    public double onBalanceVolume();

}
