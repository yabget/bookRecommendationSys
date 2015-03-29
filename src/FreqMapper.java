import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ydubale on 3/29/15.
 *
 * Input - lines from each book
 *          Key - Not used
 *          Value - line
 * Output - A intermediate mapping
 *          Key - Word (space) Title
 *          Value - 1
 *
 */
public class FreqMapper extends Mapper<Object, Text, Text, IntWritable> {

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