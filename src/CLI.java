import java.io.*;
import java.util.*;

/**
 * Created by ydubale on 3/20/15.
 */
public class CLI {

    private final int numSimilarBooksToReturn = 2;

    private final HashMap<String, Map<String, Double>> euclideanMatrix = new HashMap<String, Map<String, Double>>();;

    public CLI(String fileName) throws FileNotFoundException {
        File mapReduceJobOutput = new File(fileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(mapReduceJobOutput));

        try {
            String line;
            ArrayList<String> lines = new ArrayList<String>();
            ArrayList<String> bookNames = new ArrayList<String>();
            while((line = bufferedReader.readLine()) != null){
                lines.add(line);
                bookNames.add(line.split("\\s")[0]);
            }

            for(int i=0; i < lines.size(); i++){
                String[] bookVals = lines.get(i).split("\\s+");

                String book = bookVals[0];

                Map<String, Double> bookToVal = euclideanMatrix.get(book);
                if(bookToVal == null){
                    bookToVal = new HashMap<String, Double>();
                }

                for(int j=1; j < bookVals.length; j++){
                    double val = Double.parseDouble(bookVals[j]);
                    bookToVal.put(bookNames.get(j-1), val);         //Get the corresponding column book
                }

                euclideanMatrix.put(book, bookToVal);
            }

            /*for(String book: euclideanMatrix.keySet()){
                System.out.println(book);
                System.out.println("\t" + euclideanMatrix.get(book));
            }*/

        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("Could not read line in file.");
            System.exit(1);
        }
    }

    private void getSimilarBooks(String bookToSearch) {
        if(!euclideanMatrix.containsKey(bookToSearch)){
            System.out.println("Book is not found.");
            return;
        }

        HashSet<String> similarBooks = new HashSet<String>();

        double leastSimilarVal = -1;
        String leastSimilarBook = new String();
        Map<String, Double> booksInRow = euclideanMatrix.get(bookToSearch);

        for(String book : booksInRow.keySet()){
            if(book.equalsIgnoreCase(bookToSearch)){
                continue;
            }
            if(similarBooks.size() >= numSimilarBooksToReturn){ //If the size of our results to return is reached

                if(booksInRow.get(book) > leastSimilarVal){ //If this book is less similar than the leastSimilar
                    continue; //Don't add book
                }

                similarBooks.remove(leastSimilarBook); //Remove the least similar book
                similarBooks.add(book); //Add the new book

                if(booksInRow.get(book) > leastSimilarVal){ //Comparing euclidean distance
                    leastSimilarVal = booksInRow.get(book);
                    leastSimilarBook = book;
                }
                continue;
            }
            if(booksInRow.get(book) > leastSimilarVal){ //Comparing euclidean distance
                leastSimilarVal = booksInRow.get(book);
                leastSimilarBook = book;
            }
            similarBooks.add(book);

        }
        for(String book : similarBooks){
            System.out.println("\t" + book);
        }

    }

    public static void main(String[] args){

        try {
            CLI cli = new CLI("bookMatrix");

            Scanner scan = new Scanner(System.in);

            while(true){
                System.out.println("Enter the name of a book: ");
                String bookToSearch = scan.next();
                cli.getSimilarBooks(bookToSearch);
            }

        } catch (FileNotFoundException e) {
            System.out.println("Input file is not found! " + args[0]);
            System.exit(1);
        }

    }


}
