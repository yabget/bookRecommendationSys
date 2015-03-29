import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Collates output of mapper.
 * Input -
 *      Key - Term/Word
 *      Value - List of Strings "|book_normalizedFreq"
 * Output -
 *      Key - Term/Word
 *      Value - Concatenation of values from input
 * Example:
 *      A |L_0.5|K_0.4450|M_0.2341
 */
public class TermFreqReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        StringBuilder output = new StringBuilder();

        for(Text bookNormalizedFreq : value){
            output.append(bookNormalizedFreq); // Concats values
        }

        context.write(key, new Text(output.toString()));
    }
}