package backend.infrastructure.ai;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class StepResult {
    private final Dataset<Row> nextState;
    private final double reward;
    private final boolean done;

    public StepResult(Dataset<Row> nextState, double reward, boolean done) {
        this.nextState = nextState;
        this.reward = reward;
        this.done = done;
    }

    public Dataset<Row> getNextState() { return nextState; }
    public double getReward() { return reward; }
    public boolean isDone() { return done; }
}
