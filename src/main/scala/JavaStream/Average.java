package JavaStream;

import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

/**
 * Created by ramakrishna on 1/10/17.
 */
public class Average implements Serializable {

    public double total;
    public double num;

    public Average(double total, double num) {
        this.total = total;
        this.num = num;
    }

    public double avg() {
        return total / num;
    }

    public Function2<Average, Integer, Average> addcount =
            new Function2<Average, Integer, Average>() {
                public Average call(Average a, Integer x) {
                    a.total += x;
                    a.num += 1;
                    return a;
                }
            };

  public   Function2<Average, Average, Average> combine =
            new Function2<Average, Average, Average>() {
                public Average call(Average a, Average b) {
                    a.total += b.total;
                    a.num += b.num;
                    return a;
                }


            };


}