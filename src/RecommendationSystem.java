import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
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
                    //Remove all white space from title, lowerCase
                    title = currLine.split("Title:")[1].replaceAll("\\s+", "").toLowerCase();
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

                String bookFraction = "|" + book + "_" + termFreq.get(term) + "/" + maxFreq;
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
            StringBuilder output = new StringBuilder();

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
        //  <L, <A, 0.4> <B, 0.5>>
        private Map<String, Map<String, Double>> bookToTermNorm = new HashMap<String, Map<String, Double>>();

        public double getDouble(String text){
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
                StringBuilder bcvLine = new StringBuilder();

                for(String term : termToBookOccurance.keySet()){
                    Double TF = termToNorm.get(term); //The term frequency for term i (in current book)

                    if(TF == null){ // The book does not contain the term (so frequency is 0)
                        //bcvLine.append(term.trim()).append("_0").append(" ");
                        continue;
                    }

                    //Get the inverse document frequency (total numBooks divided by number of books term i appears in
                    double IDF = Math.log((double)numBooks/ termToBookOccurance.get(term)) / Math.log(2);

                    double TFIDF = TF * IDF;
                    if(TFIDF == 0){
                        //bcvLine.append(term.trim()).append("_0").append(" ");
                        continue;
                    }

                    bcvLine.append(term.trim()).append("=").append(TFIDF).append(" ");
                }

                //Write book followed by space separated values
                context.write(new Text(book), new Text(bcvLine.toString()));
            }
        }
    }

    public static class EuclidDMapper extends Mapper<LongWritable, Text, Text, Text> {

        private RandomAccessFile randomAccessFile;

        public void getBookBCV(String[] line, HashMap<String, Double> termToTFIDF){
            for(int i=1; i < line.length; ++i){
                String[] keyVal = line[i].split("=");
                termToTFIDF.put(keyVal[0], Double.parseDouble(keyVal[1]));
            }
        }

        public void setup(Context context) throws FileNotFoundException {
            randomAccessFile = new RandomAccessFile("./fourthInput", "r");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            randomAccessFile.seek(key.get());

            String[] bookBCVs = randomAccessFile.readLine().split("\\s+");
            String currBook = bookBCVs[0];

            HashMap<String, Double> book1 = new HashMap<String, Double>();
            getBookBCV(bookBCVs, book1);

            String line;
            context.write(new Text(currBook + "_" + currBook), new Text("0 0"));

            while((line = randomAccessFile.readLine()) != null){
                String[] otherBookBCV = line.split("\\s+");
                String otherBook = otherBookBCV[0];

                HashMap<String, Double> book2 = new HashMap<String, Double>();
                getBookBCV(otherBookBCV, book2);

                Text outKey = new Text(currBook + "_" + otherBook);

                for(String b1Val : book1.keySet()){
                    StringBuilder stringBuilder = new StringBuilder();
                    if(book2.containsKey(b1Val)){
                        stringBuilder.append(book1.get(b1Val)).append(" ").append(book2.get(b1Val));
                    }
                    else{
                        stringBuilder.append(book1.get(b1Val)).append(" ").append(0);
                    }

                    context.write(outKey, new Text(stringBuilder.toString()));
                }
            }
        }
    }

    public static class EuclidDPartioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numReducers) {
            return key.toString().split("_")[1].hashCode() % numReducers;
        }
    }

    public static class EuclidDReducer extends Reducer<Text ,Text, Text, Text>{

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

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();

        conf.setLong("mapred.task.timeout", 1800000);

        Job job = Job.getInstance(conf, "Freq Job");

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

        Job secJob = Job.getInstance(conf, "TermFreq Job");

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

        Job thrJob = Job.getInstance(conf, "BCV Job");

        thrJob.setJarByClass(RecommendationSystem.class);

        thrJob.setMapperClass(BCVMapper.class);
        thrJob.setMapOutputKeyClass(Text.class);
        thrJob.setMapOutputValueClass(Text.class);

        Path thirdJobInputPath = new Path("/recSys/secondJobOutput/part-r-00000");
        Path thirdJobOutputPath = new Path("/recSys/thirdJobOutput");

        FileInputFormat.setInputPaths(thrJob, thirdJobInputPath);
        FileOutputFormat.setOutputPath(thrJob, thirdJobOutputPath);

        thrJob.waitForCompletion(true);

        // FOURTH JOB

        conf.setInt("mapred.reduce.tasks", 40);
        Job fourthJob = Job.getInstance(conf, "EuclidD Job");

        fourthJob.setJarByClass(RecommendationSystem.class);

        fourthJob.setInputFormatClass(NLineInputFormat.class);

        NLineInputFormat.addInputPath(fourthJob, new Path("/recSys/thirdJobOutput/part-r-00000"));

        // Each line of the file gets sent to a mapper
        fourthJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

        System.out.println("SETUP NLINEINPUT FORMAT!");

        fourthJob.setMapperClass(EuclidDMapper.class);

        fourthJob.setMapOutputKeyClass(Text.class);
        fourthJob.setMapOutputValueClass(Text.class);

        fourthJob.setReducerClass(EuclidDReducer.class);

        Path fourthJobInputPath = new Path("/recSys/thirdJobOutput/part-r-00000");
        fourthJob.addCacheFile(new URI("/recSys/thirdJobOutput/part-r-00000#fourthInput"));

        Path fourthJobOutputPath = new Path("/recSys/fourthJobOutput");

        FileInputFormat.setInputPaths(fourthJob, fourthJobInputPath);
        FileOutputFormat.setOutputPath(fourthJob, fourthJobOutputPath);

        fourthJob.waitForCompletion(true);

    }

    /*
    public static class EuclideanDistanceMapper extends Mapper<Object, Text, Text, Text> {

        HashMap<String, String> comparedBooks = new HashMap<String, String>();

        public void getBookBCV(String[] line, HashMap<String, Double> tempBCV){
            for(int i=1; i < line.length; ++i){
                String[] keyVal = line[i].split("=");
                tempBCV.put(keyVal[0], Double.parseDouble(keyVal[1]));
            }
        }

        public double euclideanDistance(double[] book1, double[] book2){
            if(book1.length != book2.length){
                System.out.println("NOT SAME LENGTH FOR EUCLIDEAN DISTANCE!");
                System.exit(1);
            }
            double euclideanD = 0;
            for(int i=0; i < book1.length; ++i){
                double diff = book1[i] - book2[i];
                double diffSquared = diff * diff;
                euclideanD += diffSquared;
            }
            return Math.sqrt(euclideanD);
        }

        public double euclidDist(HashMap<String, Double> book1, HashMap<String, Double> book2){
            double euclidD= 0;
            for(String b1Val : book1.keySet()){
                if(book2.containsKey(b1Val)){
                    double diff = book1.get(b1Val) - book2.get(b1Val);
                    euclidD += (diff * diff);
                }
            }
            return Math.sqrt(euclidD);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            LineIterator lineIterator = FileUtils.lineIterator(new File("./fourthInput"));

            String[] spaceSplit = value.toString().split("\\s+");
            String currBook = spaceSplit[0];

            HashMap<String, Double> currentBook = new HashMap<String, Double>();
            getBookBCV(spaceSplit, currentBook);

            StringBuilder stringBuilder = new StringBuilder();
            try{
                while (lineIterator.hasNext()){
                    String[] line = lineIterator.nextLine().split("\\s+");
                    String book = line[0];

                    if(comparedBooks.containsKey(book)){
                        continue;
                    }

                    HashMap<String, Double> newBook = new HashMap<String, Double>();
                    getBookBCV(line, newBook);

                    double euclidD = euclidDist(currentBook, newBook);
                    stringBuilder.append(euclidD).append(" ");
                }
            }
            finally {
                LineIterator.closeQuietly(lineIterator);
            }
            context.write(new Text(currBook), new Text(stringBuilder.toString()));
            comparedBooks.put(currBook, null);
        }
    }
    */

    /*
    public static class EuclidDMapper extends Mapper<Object, Text, Text, DoubleArrayWritable> {

        public void getBookBCV(String[] line, HashMap<String, Double> termToTFIDF){
            for(int i=1; i < line.length; ++i){
                String[] keyVal = line[i].split("=");
                termToTFIDF.put(keyVal[0], Double.parseDouble(keyVal[1]));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] bookBCVs = value.toString().split("\\s+");
            String currBook = bookBCVs[0];

            HashMap<String, Double> book1 = new HashMap<String, Double>();
            getBookBCV(bookBCVs, book1);

            boolean startWriting = false;

            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("./fourthInput")));

            String line;
            DoubleWritable[] outValue = new DoubleWritable[2];

            while((line = bufferedReader.readLine()) != null){
                String[] otherBookBCV = line.split("\\s+");
                String otherBook = otherBookBCV[0];
                if(currBook.equals(otherBook)){
                    startWriting = true;
                }
                if(startWriting){
                    HashMap<String, Double> book2 = new HashMap<String, Double>();
                    getBookBCV(otherBookBCV, book2);

                    Text outKey = new Text(currBook + "_" + otherBook);

                    for(String b1Val : book1.keySet()){
                        if(book2.containsKey(b1Val)){
                            outValue[1] = new DoubleWritable(book2.get(b1Val));
                        }
                        else{
                            outValue[1] = new DoubleWritable(0);
                        }
                        outValue[0] = new DoubleWritable(book1.get(b1Val));
                        context.write(outKey, new DoubleArrayWritable(outValue));
                    }
                }
            }
        }
    }
    */

}
