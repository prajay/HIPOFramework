import java.io.Serializable;

/**
 * Created by dreamlab2 on 4/4/17.
 */
public class TimeHints implements Serializable {
    Long minTime;
    Integer delta;

    public TimeHints(Long minTime, Integer delta) {
        this.minTime = minTime;
        this.delta = delta;
    }

    public Long getMinTime() {
        return minTime;
    }

    public Integer getDelta() {
        return delta;
    }

    public void setMinTime(Long minTime) {
        this.minTime = minTime;
    }

    public void setDelta(Integer delta) {
        this.delta = delta;
    }
}
