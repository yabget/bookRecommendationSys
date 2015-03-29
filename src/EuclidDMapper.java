import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * Outputs:
 * Book1_Book2 TF.IDF1 TFIDF2
 * Book1_Book3 TF.IDF1 TFIDF3
 * ...
 */
public class EuclidDMapper extends Mapper<LongWritable, Text, Text, Text> {

    private RandomAccessFile randomAccessFile;

    /**
     * Populates the given Map
     * @param line - line of input file (book1 term1=TF.IDF term3=TF.IDF ... )
     * @param termToTFIDF - Map of all terms and corresponding TF.IDFs
     */
    private void getBookBCV(String[] line, Map<String, Double> termToTFIDF){
        for(int i=1; i < line.length; ++i){
            String[] keyVal = line[i].split("=");
            termToTFIDF.put(keyVal[0], Double.parseDouble(keyVal[1]));
        }
    }

    /**
     * Initializes the randomAccessFile for reading the distributed cache file
     * @param context
     * @throws FileNotFoundException
     */
    public void setup(Context context) throws FileNotFoundException {
        randomAccessFile = new RandomAccessFile("./fourthInput", "r");
    }

    /**
     * Outputs key two books and their TF.IDF for the term
     * @param key - The offset of the line in the file
     * @param value - Lines of the file (book1 term1=TF.IDF term3=TF.IDF ... )
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        randomAccessFile.seek(key.get()); //Jump to the line in the file to read the rest of it
        //This is to allow comparison of the current book with the rest of the books in the file

        String[] bookBCVs = randomAccessFile.readLine().split("\\s+");
        String currBook = bookBCVs[0];

        HashMap<String, Double> book1 = new HashMap<String, Double>();
        getBookBCV(bookBCVs, book1);

        String line;
        //Write the current book as similar to itself
        context.write(new Text(currBook + "_" + currBook), new Text("0 0"));

        //Read the rest of the book
        while((line = randomAccessFile.readLine()) != null){

            String[] otherBookBCV = line.split("\\s+");
            String otherBook = otherBookBCV[0];

            HashMap<String, Double> book2 = new HashMap<String, Double>();
            getBookBCV(otherBookBCV, book2);

            Text outKey = new Text(currBook + "_" + otherBook);

            for(String term : book1.keySet()){
                StringBuilder stringBuilder = new StringBuilder();

                if(book2.containsKey(term)){ //If the other book also has a TF.IDF value for the term
                    stringBuilder.append(book1.get(term)).append(" ").append(book2.get(term));
                }
                else{
                    //If not, put 0 for the second book
                    stringBuilder.append(book1.get(term)).append(" ").append(0);
                }

                context.write(outKey, new Text(stringBuilder.toString()));
            }
        }
    }
}