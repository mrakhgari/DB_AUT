

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("start of project, reading data from file");

        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try (final Options options = new Options().setCreateIfMissing(true)) {

            // a factory method that returns a RocksDB instance
            try (final RocksDB db = RocksDB.open(options, "./dbFiles.d")) {
                try {
                    addCSVFileToDB(db, args[0]);
                } catch (FileNotFoundException file) {
                    System.out.println("can't find csv file ");
                } catch (RocksDBException r) {
                    System.out.println(" can't add to db");
                }
            }
        } catch (RocksDBException e) {
            // do some error handling
            System.out.println("can't open the db");
        }
    }

    /**
     * a function that reads {@code string} key and {@code string}value and add
     * to db instance.
     *
     * @param db      a db instance from {@code RocksDb}.
     * @param csvPath a {@code} string that show path of csv file.
     */
    public static void addCSVFileToDB(RocksDB db, String csvPath) throws FileNotFoundException, RocksDBException {
        Scanner csvReader = new Scanner(new File(csvPath));
        while (csvReader.hasNext()) {
            String[] line = csvReader.nextLine().split(",");
//            System.out.println(Arrays.toString(line));
            db.put(line[0].getBytes(), line[1].getBytes());
        }
        System.out.println("added to db");
    }

}
