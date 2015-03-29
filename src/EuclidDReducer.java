import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EuclidDReducer extends Reducer<Text ,Text, Text, Text> {

    private double euclidDist(String[] nums){
        double num1 = Double.parseDouble(nums[0]);
        double num2 = Double.parseDouble(nums[1]);

        double diff = num1 - num2;

        return diff * diff;
    }

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        double sum = 0;
        for(Text vals : value){
            sum += euclidDist(vals.toString().split("\\s+"));
        }
        double euclidD = Math.sqrt(sum);

        context.write(key, new Text(euclidD + ""));

    }
}
