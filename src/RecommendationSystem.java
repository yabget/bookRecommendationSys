import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ydubale on 3/17/15.
 */
public class RecommendationSystem {

    /**
     * Input - lines from each book
     *          Key - Not used
     *          Value - line
     * Output - A intermediate mapping
     *          Key - Word (space) Title
     *          Value - 1
     */
    public static class FreqMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);

        private boolean startParsing = false;

        private String title = "UNKNOWN"; //Default if the title of book DNE

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String currLine = value.toString();

            if(currLine == null || currLine.isEmpty()){
                return;
            }

            if(currLine.contains("*** START OF")){ //Everything before this line is ignored
                startParsing = true;
                return;
            }

            if(currLine.contains("*** END OF")){ //Everything after this line is ignored
                startParsing = false;
                return;
            }

            if(startParsing == false){
                if(currLine.contains("Title:")){
                    title = currLine.split("Title:")[1].replaceAll("\\s+", ""); //Remove all white space from title
                }
                return; //Don't start parsing yet
            }

            for(String token : currLine.split("\\s")){ //Seperate each line by space

                if(token.equals(null)) continue;

                token = token.replaceAll("[^a-zA-Z1-9]", "").toLowerCase().trim(); //remove all characters but a-z (1-9)

                if(token.isEmpty()) continue;

                context.write(new Text(token + " " + title), one);
            }
        }
    }

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
    public static class FreqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable val : value){
                sum += val.get();
            }

            context.write(new Text(key), new IntWritable(sum));
        }
    }

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
    public static class TermFreqMapper extends Mapper<Object, Text, Text, Text> {

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

                String bookFraction = "|" + book + "_" + ((double)termFreq.get(term)/ maxFreq);
                // |L_(10/maxFreq)

                context.write(new Text(word), new Text(bookFraction)); // <A, "|L_(10/maxFreq)">
            }
        }
    }

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
    public static class TermFreqReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            StringBuffer output = new StringBuffer(); //For a more efficient append

            for(Text bookNormalizedFreq : value){
                output.append(bookNormalizedFreq); // Concats values
            }

            context.write(key, new Text(output.toString()));
        }
    }

    /**
     * Cleanup is used because a list of all the terms and books has to be collected before calculating the
     * Book Characteristic Vector.
     *
     * Calculates TFxIDF for every term.
     *
     * Output: A matrix of books to TF.IDF of every word found. If word does not exist in the book, TF.IDF is 0.
 *          TF.IDF(word1) TF.IDF(word2) TF.IDF(word3) TF.IDF(word4) ... TF.IDF(wordm)
     * Book1
     * Book2
     * Book3
     * ...
     * Bookn
     *
     */
    public static class BCVMapper extends Mapper<Object, Text, Text, Text> {

        //Maps a term to the number of books the term appears in
        //Ex. If A appears in 20 book <A, 20>
        private Map<String, Integer> termToBookOccurance = new HashMap<String, Integer>();

        //Maps a book to a hashmap of the < term, normalizedFreq > for that book
        //Ex. If A has normalized frequency of 0.4 for book L
        //  <L, <A, 0.4>>
        private Map<String, Map<String, Double>> bookToTermNorm = new HashMap<String, Map<String, Double>>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] wordNormalizedfreqs = value.toString().split("\\|"); //Ex. A |L_0.5|K_0.4450|M_0.2341

            String term = wordNormalizedfreqs[0]; //A
            termToBookOccurance.put(term, wordNormalizedfreqs.length - 1); //Number of books term appears in (3)

            for(int i = 1; i < wordNormalizedfreqs.length; i++){

                String[] bookNormFreq = wordNormalizedfreqs[i].split("_"); //Seperate book and normalizedFreq

                String book = bookNormFreq[0];
                double normFreq = Double.parseDouble(bookNormFreq[1]);

                //Gets the of terms to frequencies for the book
                Map<String, Double> termToNorm = bookToTermNorm.get(book);

                if(termToNorm == null){ //Create a new map for the book if it has not already been created
                    termToNorm = new HashMap<String, Double>();
                    bookToTermNorm.put(book, termToNorm);
                }

                termToNorm.put(term, normFreq); //Puts the term and normFreq in map of corresponding book
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            final Set<String> books = bookToTermNorm.keySet();
            final int numBooks = books.size();

            for(String book : books){
                Map<String, Double> termToNorm = bookToTermNorm.get(book); //All terms and normFreq in book
                StringBuffer bcvLine = new StringBuffer();

                for(String term : termToBookOccurance.keySet()){
                    Double TF = termToNorm.get(term); //The term frequency for term i (in current book)

                    if(TF == null){ // The book does not contain the term (so frequency is 0)
                        bcvLine.append("0 ");
                        continue;
                    }

                    //Get the inverse document frequency (total numBooks divided by number of books term i appears in
                    double IDF = Math.log((double)numBooks/ termToBookOccurance.get(term)) / Math.log(2);

                    bcvLine.append(TF*IDF + " ");
                }

                //Write book followed by space separated values
                context.write(new Text(book), new Text(bcvLine.toString()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TermFrequency");

        job.setJarByClass(RecommendationSystem.class);

        job.setMapperClass(FreqMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(FreqReducer.class);

        job.setReducerClass(FreqReducer.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        // SECOND JOB

        Job secJob = Job.getInstance(conf, "TermFrequency");

        secJob.setJarByClass(RecommendationSystem.class);

        secJob.setMapperClass(TermFreqMapper.class);
        secJob.setMapOutputKeyClass(Text.class);
        secJob.setMapOutputValueClass(Text.class);

        secJob.setCombinerClass(TermFreqReducer.class);

        secJob.setReducerClass(TermFreqReducer.class);

        Path secondJobInputPath = new Path("/recSys/firstJobOutput/part-r-00000");
        Path secondJobOutputPath = new Path("/recSys/secondJobOutput");

        FileInputFormat.setInputPaths(secJob, secondJobInputPath);
        FileOutputFormat.setOutputPath(secJob, secondJobOutputPath);

        secJob.waitForCompletion(true);


        // THIRD JOB

        Job thrJob = Job.getInstance(conf, "TermFrequency");

        thrJob.setJarByClass(RecommendationSystem.class);

        thrJob.setMapperClass(BCVMapper.class);
        thrJob.setMapOutputKeyClass(Text.class);
        thrJob.setMapOutputValueClass(Text.class);

        Path thirdJobInputPath = new Path("/recSys/secondJobOutput/part-r-00000");
        Path thirdJobOutputPath = new Path("/recSys/thirdJobOutput");

        FileInputFormat.setInputPaths(thrJob, thirdJobInputPath);
        FileOutputFormat.setOutputPath(thrJob, thirdJobOutputPath);

        thrJob.waitForCompletion(true);


    }

}
