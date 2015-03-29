import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Input - from FreqMapper
 *          Key - Word (space) Title
 *          Value - List of 1s for each pairing of word and title
 * Output - Unigram of words in each book
 *          Key - Word (space) Title
 *          Value - Occurrences of word in corresponding book
 * Example output:
 * A appears in book L 10 times, in book K 12 times
 *      A L 10
 *      A K 12
 */
public class FreqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for(IntWritable val : value){
            sum += val.get();
        }

        context.write(new Text(key), new IntWritable(sum));
    }
}
