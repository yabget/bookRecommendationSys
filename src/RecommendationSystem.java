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

    public static class FreqMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);

        private boolean startParsing = false;
        private String title = "UNKNOWN";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String currLine = value.toString();

            if(currLine == null || currLine.isEmpty()){
                return;
            }

            if(currLine.contains("*** START OF")){ //Start of book
                startParsing = true;
            }

            if(currLine.contains("*** END OF")){ //End of book
                startParsing = false;
            }

            if(startParsing == false){
                if(currLine.contains("Title:")){
                    title = currLine.split("Title:")[1].replaceAll("\\s+", ""); //Remove all white space from title
                }
                return;
            }

            for(String token : currLine.split("\\s")){

                if(token.equals(null)) continue;

                token = token.replaceAll("[^a-zA-Z1-9]", "").toLowerCase().trim(); //remove all characters but a-z (1-9)

                if(token.isEmpty()) continue;

                context.write(new Text(token + " " + title), one);

            }
        }
    }

    public static class FreqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable val : value){
                sum += val.get();
            }

            context.write(new Text(key), new IntWritable(sum));
        }
    }

    public static class TermFreqMapper extends Mapper<Object, Text, Text, Text> {

        private int maxFreq = 0;
        private Map<String, Integer> termFreq = new HashMap<String, Integer>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordBookFreq = value.toString().split("\\s");
            String term = wordBookFreq[0];
            String book = wordBookFreq[1];
            int freq = Integer.parseInt(wordBookFreq[2]);

            if(freq > maxFreq){
                maxFreq = freq;
            }

            termFreq.put(term + " " + book, freq);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for(String term : termFreq.keySet()) {
                String[] termBook = term.split("\\s");
                String word = termBook[0];
                String book = termBook[1];

                String bookFraction = "|" + book + "_" + ((double)termFreq.get(term)/ maxFreq);

                context.write(new Text(word), new Text(bookFraction));
            }
        }
    }

    public static class TermFreqReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            StringBuffer output = new StringBuffer();
            for(Text bookNormalizedFreq : value){
                output.append(bookNormalizedFreq);
            }

            context.write(key, new Text(output.toString()));
        }
    }

    public static class DFMapper extends Mapper<Object, Text, Text, Text> {

        private static final IntWritable one = new IntWritable(1);
        private Map<String, Integer> terms = new HashMap<String, Integer>();
        private Map<String, Map<String, Double>> bookToTermNorm = new HashMap<String, Map<String, Double>>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordNormalizedfreqs = value.toString().split("\\|");
            String term = wordNormalizedfreqs[0];
            terms.put(term, wordNormalizedfreqs.length - 1); //Number of books term appears in

            for(int i = 1; i < wordNormalizedfreqs.length; i++){
                String[] bookNormFreq = wordNormalizedfreqs[i].split("_");
                String book = bookNormFreq[0];
                double normFreq = Double.parseDouble(bookNormFreq[1]);

                Map<String, Double> termToNorm = bookToTermNorm.get(book);

                if(termToNorm == null){
                    termToNorm = new HashMap<String, Double>();
                    bookToTermNorm.put(book, termToNorm);
                }

                termToNorm.put(term, normFreq);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> books = bookToTermNorm.keySet();
            final int numBooks = books.size();

            for(String book : books){
                Map<String, Double> termToNorm = bookToTermNorm.get(book);
                StringBuffer bcvLine = new StringBuffer();

                for(String term : terms.keySet()){
                    Double TF = termToNorm.get(term);

                    if(TF == null){ // Put 0 for value
                        bcvLine.append("0.0 ");
                        continue;
                    }

                    double IDF = Math.log((double)numBooks/ terms.get(term)) / Math.log(2); //Take log base 2

                    bcvLine.append(TF*IDF + " ");
                }
                context.write(new Text(bcvLine.toString()), new Text(""));
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

        thrJob.setMapperClass(DFMapper.class);
        thrJob.setMapOutputKeyClass(Text.class);
        thrJob.setMapOutputValueClass(Text.class);

        //thrJob.setCombinerClass(DFReducer.class);

        //thrJob.setReducerClass(DFReducer.class);

        Path thirdJobInputPath = new Path("/recSys/secondJobOutput/part-r-00000");
        Path thirdJobOutputPath = new Path("/recSys/thirdJobOutput");

        FileInputFormat.setInputPaths(thrJob, thirdJobInputPath);
        FileOutputFormat.setOutputPath(thrJob, thirdJobOutputPath);

        thrJob.waitForCompletion(true);


    }

}
