import java.io.Serializable;

/**
 * Created by dreamlab2 on 4/29/17.
 */
public class PairKey implements Comparable<PairKey>, Serializable {
    long ts;
    long id;

    public PairKey(long ts, long id) {
        this.ts = ts;
        this.id = id;
    }

    @Override
    public int compareTo(PairKey pairKey) {
        int compare = Long.compare(this.ts, pairKey.ts);
        if (compare != 0) {
            return compare;
        } else {
            return Long.compare(this.id, pairKey.id);
        }
    }

    @Override
    public boolean equals(Object obj) {
        PairKey p = (PairKey)obj;
        if ((Long.compare(this.ts, p.ts) == 0) && (Long.compare(this.id, p.id) == 0))
            return true;
        else
            return false;

    }

    @Override
    public int hashCode() {

//        System.out.println("hashcode: " + (int)(ts * id));
        return (int) (ts + id);
    }
}
