import java.io.*;
import java.util.*;

/**
 * Created by ydubale on 3/20/15.
 */
public class CLI {

    private final int numSimilarBooksToReturn = 30;

    //Map <book, <books, similiartyValues>>. Essentially a matrix
    private final HashMap<String, Map<String, Double>> euclideanMatrix = new HashMap<String, Map<String, Double>>();

    /**
     * returns the row of books and values for the given bookName
     * @param bookName
     * @return
     */
    private Map<String, Double> getMatrixRow(String bookName){
        Map<String, Double> books = euclideanMatrix.get(bookName);

        if(books == null){
            books = new HashMap<String, Double>();
        }

        return books;
    }

    /**
     * File format:
     * book1_book2 value1
     * book1_book3 value2
     * ...
     *
     * @param fileName
     * @throws IOException
     */
    public CLI(String fileName) throws IOException {
        File mapReduceJobOutput = new File(fileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(mapReduceJobOutput));

        try {
            String line;
            while((line = bufferedReader.readLine()) != null){
                String[] spaceSep = line.split("\\s+"); //splits [book1_book2, value1]
                String[] books = spaceSep[0].split("_"); // [book1, book2]

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
                System.out.println("Title: " + name); //Print all available titles
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

    /**
     * Prints out a list of similar books to given bookToSearch
     * @param bookToSearch
     */
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

            //If the size of our results to return is reached
            if(similarBooks.size() >= numSimilarBooksToReturn){

                similarBooks.add(book); //Add the extra book
                double leastSimVal = -1;

                String toRemove = book;

                //Find the least similar book to the bookToSearch
                for(String similarBook : similarBooks){
                    double bookVal = booksInRow.get(similarBook);
                    if(bookVal > leastSimVal){
                        leastSimVal = bookVal;
                        toRemove = similarBook;
                    }
                }

                //Remove that book
                similarBooks.remove(toRemove);
                continue;
            }

            // Adds the first x number of books. x = numSimilarBooksToReturn
            similarBooks.add(book);

        }

        int count = 1;

        String[] simBooks = new String[similarBooks.size()];
        similarBooks.toArray(simBooks);
        Arrays.sort(simBooks);

        //Sort and print similar books
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
