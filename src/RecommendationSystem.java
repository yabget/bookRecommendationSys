import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by ydubale on 3/17/15.
 */
public class RecommendationSystem {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();

        conf.setLong("mapred.task.timeout", 1800000);

        runFirstJob(args, conf); //Calculates unigram of words per book

        runSecondJob(conf); // Calculates the normalized term frequency

        runThirdJob(conf); // Calculates the book characteristic vector

        conf.setInt("mapred.reduce.tasks", 50); //Running on ~50 nodes
        runFourthJob(conf); // Creates the similarity matrix of books

    }

    private static void runFourthJob(Configuration conf) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
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

    private static void runThirdJob(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
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
    }

    private static void runSecondJob(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
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
    }

    private static void runFirstJob(String[] args, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
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
    }

}
