import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**
 *
 */
public class EuclidDMapper extends Mapper<LongWritable, Text, Text, Text> {

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