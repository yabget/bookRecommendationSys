import java.io.*;
import java.util.*;

/**
 * Created by ydubale on 3/20/15.
 */
public class CLI {

    private final int numSimilarBooksToReturn = 30;

    private final HashMap<String, Map<String, Double>> euclideanMatrix = new HashMap<String, Map<String, Double>>();;

    private Map<String, Double> getMatrixRow(String bookName){
        Map<String, Double> book = euclideanMatrix.get(bookName);

        if(book == null){
            book = new HashMap<String, Double>();
        }

        return book;
    }

    public CLI(String fileName) throws IOException {
        File mapReduceJobOutput = new File(fileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(mapReduceJobOutput));

        try {

            String line;
            while((line = bufferedReader.readLine()) != null){
                String[] spaceSep = line.split("\\s+");
                String[] books = spaceSep[0].split("_");

                //Book[0] is first book, book[1] is second book
                Map<String, Double> book1 = getMatrixRow(books[0]);
                Map<String, Double> book2 = getMatrixRow(books[1]);

                double similarity = Double.parseDouble(spaceSep[1]);

                book1.put(books[1], similarity);
                book2.put(books[0], similarity);

                euclideanMatrix.put(books[0], book1);
                euclideanMatrix.put(books[1], book2);

            }

            System.out.println("Indexed " + euclideanMatrix.size() + " books.");
            for(String name : euclideanMatrix.keySet()){
                System.out.println("Title: " + name);
            }
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("Could not read line in file.");
            System.exit(1);
        }
        finally {
            bufferedReader.close();
        }
    }

    private void getSimilarBooks(String bookToSearch) {
        if(!euclideanMatrix.containsKey(bookToSearch)){
            System.out.println("Book is not found. Check spelling.");
            return;
        }

        HashSet<String> similarBooks = new HashSet<String>();

        Map<String, Double> booksInRow = euclideanMatrix.get(bookToSearch);

        for(String book : booksInRow.keySet()){
            if(book.equalsIgnoreCase(bookToSearch)){
                continue;
            }

            if(similarBooks.size() >= numSimilarBooksToReturn){ //If the size of our results to return is reached

                similarBooks.add(book); //Add the book
                double leastSimVal = -1;

                String toRemove = book;
                for(String similarBook : similarBooks){
                    double bookVal = booksInRow.get(similarBook);
                    if(bookVal > leastSimVal){
                        leastSimVal = bookVal;
                        toRemove = similarBook;
                    }
                }
                similarBooks.remove(toRemove);
                continue;
            }

            similarBooks.add(book);

        }
        int count = 1;
        String[] simBooks = new String[similarBooks.size()];
        similarBooks.toArray(simBooks);
        Arrays.sort(simBooks);
        for(String book : simBooks){
            System.out.println("\t" + count + " " + book);
            count++;
        }

    }

    public static void main(String[] args){


        try {
            CLI cli = new CLI("FINAL_600.txt");

            Scanner scan = new Scanner(System.in);

            while(true){
                System.out.println("Enter the name of a book: ");
                String bookToSearch = scan.next();
                cli.getSimilarBooks(bookToSearch);
            }

        } catch (FileNotFoundException e) {
            System.out.println("Input file is not found! " + args[0]);
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
