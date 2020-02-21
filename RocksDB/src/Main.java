

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

import static java.lang.System.exit;


public class Main {
    private final static String CREATE = "create";
    private final static String FETCH = "fetch";
    private final static String UPDATE = "update";
    private final static String DELETE = "delete";

    public static void main(String[] args) {
        System.out.println("start of project, reading data from file");
        Scanner scanner = new Scanner(System.in);

        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        // the Options class contains a set of configurable DB options
        // that determines the behaviour of the database.
        try (final Options options = new Options().setCreateIfMissing(true)) {

            // a factory method that returns a RocksDB instance
            try (final RocksDB db = RocksDB.open(options, "./dbFiles.d")) {
                try {
                    addCSVFileToDB(db, args[0]);

                    while (true) {
                        parseQuery(db, scanner.nextLine().split(" "));
                    }
                } catch (FileNotFoundException file) {
                    System.out.println("can't find csv file ");
                }
            }
        } catch (RocksDBException e) {
            // do some error handling
            System.out.println("can't open the db");
        }
    }

    private static void parseQuery(RocksDB db, String[] query) throws RocksDBException {
        System.out.println("query is " + Arrays.toString(query));
        switch (query[0].toLowerCase()) {
            case CREATE:
                createInstance(db, query[1].getBytes(), query[2].getBytes());
                break;
            case FETCH:
                fetchInstance(db, query[1].getBytes());
                break;
            case UPDATE:
                updateInstance(db, query[1].getBytes(), query[2].getBytes());
                break;
            case DELETE:
                deleteInstance(db, query[1].getBytes());
                break;
            default:
                System.out.println("invalid command");
                db.close();
                exit(0);

        }
    }

    private static void deleteInstance(RocksDB db, byte[] key) throws RocksDBException {
        System.out.println("in the deleteInstance");
        byte[] value = db.get(key);
        if (value != null) {
            db.delete(key);
            System.out.println("true");
            return;
        }
        System.out.println("false");
    }

    private static void updateInstance(RocksDB db, byte[] key, byte[] value) throws RocksDBException {
        System.out.println("in the updateInstance");
        byte[] val = db.get(key);
        if (val != null) {
            System.out.println("true");
            deleteInstance(db, key);
            createInstance(db, key, value);
            System.out.println("true");
            return;
        }
        System.out.println("false");
    }

    private static void fetchInstance(RocksDB db, byte[] key) throws RocksDBException {
        System.out.println("in the fetch instance method" + Arrays.toString(key));
        byte[] value = db.get(key);
        if (value == null) {
            System.out.println("false");
            return;
        }
        System.out.println("true");
        System.out.println(new String(db.get(key)));
    }

    private static void createInstance(RocksDB db, byte[] key, byte[] value) throws RocksDBException {
        System.out.println("in the create instance");
        byte[] val = db.get(key);
        if (val == null) {
            System.out.println("true");
            db.put(key, value);
            return;
        }
        System.out.println("false");
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
            db.put(line[0].getBytes(), line[1].getBytes());
        }
        System.out.println("data added to db");
    }

}
