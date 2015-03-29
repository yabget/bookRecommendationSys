import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Calculates the normalized term frequency of word i in document j.
 *
 * Cleanup is used because the most frequent word won't be known until every word, freq pair has been parsed
 * Map is used to keep track of the < Word_Title, Freq>.
 * This is expected to fit in memory since a whole dictionary of words would be around 3-4MB.
 *
 * Input - lines from each book
 *          Key - Not used
 *          Value - line
 * Output - For each term, the normalized term frequency in each book
 *          Key - Term/Word
 *          Value - A string "|BookTitle_NormalizedFrequency"
 * Example output:
 * Term is A; Books are L, K, M
 *      A |L_0.5
 *      A |K_0.4450
 *      A |M_0.2341
 */
public class TermFreqMapper extends Mapper<Object, Text, Text, Text> {

    private int maxFreq = 0;    //Every term is expected to have freq > 0
    private Map<String, Integer> termFreq = new HashMap<String, Integer>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordBookFreq = value.toString().split("\\s"); //Ex. A L 10

        String term = wordBookFreq[0];  //A
        String book = wordBookFreq[1];  //L
        int freq = Integer.parseInt(wordBookFreq[2]);   //10

        if(freq > maxFreq){
            maxFreq = freq;
        }

        termFreq.put(term + " " + book, freq); //Build Map, //< A_L, 10>
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for(String term : termFreq.keySet()) {
            String[] termBook = term.split("\\s"); //Ex. A(space)L
            String word = termBook[0]; //A
            String book = termBook[1]; //L

            String bookFraction = "|" + book + "_" + termFreq.get(term) + "/" + maxFreq;
            // |L_(10/maxFreq)

            context.write(new Text(word), new Text(bookFraction)); // <A, "|L_(10/maxFreq)">
        }
    }
}