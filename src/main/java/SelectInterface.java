import java.io.Serializable;

/**
 * Created by dreamlab2 on 4/14/17.
 */
/*Currently needs R2 to be a Tuple type*/
public interface SelectInterface<R1,R2> extends Serializable {
    R2 select(R1 r1);
}
