import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Outputs a similarity matrix of books
 */
public class EuclidDReducer extends Reducer<Text ,Text, Text, DoubleWritable> {

    /**
     * Calculates the difference squared of two TF.IDFs
     * @param nums - array of 2 values
     * @return the difference squared of the 2 values
     */
    private double euclidDist(String[] nums){
        double num1 = Double.parseDouble(nums[0]);
        double num2 = Double.parseDouble(nums[1]);

        double diff = num1 - num2;

        return diff * diff;
    }

    /**
     * Writes out:
     * Book1_book2 similarityValue
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        double sum = 0;

        for(Text vals : value){
            sum += euclidDist(vals.toString().split("\\s+"));
        }

        double euclidD = Math.sqrt(sum);

        context.write(key, new DoubleWritable(euclidD));

    }
}
