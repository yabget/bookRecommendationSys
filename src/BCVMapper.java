import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Cleanup is used because a list of all the terms and books has to be collected before calculating the
 * Book Characteristic Vector.
 *
 * Calculates TFxIDF for every term in each book
 *
 * Output: A mapping between the term and TF.IDF value of the term for that book.
 * Ex.
 * Book1 term1=TF.IDF term2=TF.IDF term5=TF.IDF term7=TF.IDF ...
 * Book2
 * Book3
 * ...
 * Bookn
 *
 */
public class BCVMapper extends Mapper<Object, Text, Text, Text> {

    //Maps a term to the number of books the term appears in
    //Ex. If term A appears in 20 books, <A, 20>
    private Map<String, Integer> termToBookOccurance = new HashMap<String, Integer>();

    //Maps a book to a hashmap of the < term, normalizedFreq > for that book
    //Ex. If term A has normalized frequency of 0.4 for book L, and term B has 0.5
    //then  <L, [<A, 0.4>,<B, 0.5>]>
    private Map<String, Map<String, Double>> bookToTermNorm = new HashMap<String, Map<String, Double>>();

    //Input Ex. "23/242"
    private double getDouble(String text){
        String[] nums = text.split("/");
        double num1 = Double.parseDouble(nums[0]);
        double num2 = Double.parseDouble(nums[1]);
        return num1/num2;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] wordNormalizedfreqs = value.toString().split("\\|"); //Ex. A |L_0.5|K_0.4450|M_0.2341

        String term = wordNormalizedfreqs[0]; //A
        termToBookOccurance.put(term, wordNormalizedfreqs.length - 1); //Number of books term appears in (3)

        for(int i = 1; i < wordNormalizedfreqs.length; i++){

            String[] bookNormFreq = wordNormalizedfreqs[i].split("_"); //Seperate book and normalizedFreq

            String book = bookNormFreq[0];
            double normFreq = getDouble(bookNormFreq[1]);

            //Gets the of terms to frequencies mapping for the book
            Map<String, Double> termToNorm = bookToTermNorm.get(book);

            if(termToNorm == null){ //Create a new map for the book if it has not already been created
                termToNorm = new HashMap<String, Double>();
                bookToTermNorm.put(book, termToNorm);
            }

            termToNorm.put(term, normFreq); //Puts the term and normFreq in map of corresponding book
        }
    }

    /**
     * Calculates the TF.IDF of every term in every book
     */
    public void cleanup(Context context) throws IOException, InterruptedException {

        final Set<String> books = bookToTermNorm.keySet();
        final int totalNumBooks = books.size();

        for(String book : books){

            Map<String, Double> termToNorm = bookToTermNorm.get(book); //All terms and normFreq in book
            StringBuilder bcvLine = new StringBuilder();

            for(String term : termToBookOccurance.keySet()){
                Double TF = termToNorm.get(term); //The term frequency for term i (in current book)

                if(TF == null){
                    continue;
                }

                //inverse document frequency: total numBooks divided by number of books term i appears in
                double IDF = Math.log((double)totalNumBooks/ termToBookOccurance.get(term)) / Math.log(2);

                double TFIDF = TF * IDF;
                if(TFIDF == 0){
                    continue;
                }

                bcvLine.append(term.trim()).append("=").append(TFIDF).append(" ");
            }

            //Write book followed by space separated values
            context.write(new Text(book), new Text(bcvLine.toString()));
        }
    }
}
