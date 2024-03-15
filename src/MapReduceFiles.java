import java.util.*;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MapReduceFiles {

    public static void main(String[] args) {

        if (args.length < 3) { // Check if at least two thread counts and one filename is provided
            System.err.println("Usage: java MapReduceFiles <map-thread-pool-size> <reduce-thread-pool-size> <file1.txt> <file2.txt> ...");
            System.exit(1);
        }

        int mapThreadPoolSize, reduceThreadPoolSize;
        try {
            mapThreadPoolSize = Integer.parseInt(args[0]); // The first argument is the map thread pool size
            reduceThreadPoolSize = Integer.parseInt(args[1]); // The second argument is the reduce thread pool size
            if (mapThreadPoolSize <= 0 || reduceThreadPoolSize <= 0) {
                throw new NumberFormatException("Number of threads must be positive");
            }
        } catch (NumberFormatException e) {
            System.err.println("First and second arguments must be integers representing the map and reduce thread pool sizes respectively.");
            e.printStackTrace();
            System.exit(1);
            return;
        }

        Map<String, String> input = new HashMap<String, String>();

        // Iterate over all provided file names (starting from the second argument) and read each file
        for (int i = 1; i < args.length; i++) { // Start from 1 to skip thread count
            String filename = args[i];
            try {
                input.put(filename, readFile(filename));
            } catch (IOException ex) {
                System.err.println("Error reading file: " + filename + "\n" + ex.getMessage());
                ex.printStackTrace();
            }
        }

        // APPROACH #1: Brute force
        {
            long startTimeApproach1 = System.currentTimeMillis();
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                String[] words = contents.trim().split("\\s+");

                for(String word : words) {

                    Map<String, Integer> files = output.get(word);
                    if (files == null) {
                        files = new HashMap<String, Integer>();
                        output.put(word, files);
                    }

                    Integer occurrences = files.remove(file);
                    if (occurrences == null) {
                        files.put(file, 1);
                    } else {
                        files.put(file, occurrences.intValue() + 1);
                    }
                }
            }

            // show me:
            long endTimeApproach1 = System.currentTimeMillis();
            System.out.println("Approach #1: Brute Force took " + (endTimeApproach1 - startTimeApproach1) + " milliseconds.");
            System.out.println(output);

        }


        // APPROACH #2: MapReduce
        {
            long startTimeApproach2 = System.currentTimeMillis();
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                map(file, contents, mappedItems);
            }

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            // REDUCE:

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                String word = entry.getKey();
                List<String> list = entry.getValue();

                reduce(word, list, output);
            }

            long endTimeApproach2 = System.currentTimeMillis();
            System.out.println("Approach #2: MapReduce took " + (endTimeApproach2 - startTimeApproach2) + " milliseconds.");
            System.out.println(output);
        }


        List<String> filesToProcess = Arrays.asList(Arrays.copyOfRange(args, 1, args.length)); // Skip the first arg for file names

        // APPROACH #3: Distributed MapReduce
        {
            long startTimeApproach3 = System.currentTimeMillis();
            final Map<String, Map<String, Integer>> output = new HashMap<>();

            // MAP:
            final List<MappedItem> mappedItems = Collections.synchronizedList(new LinkedList<>()); // Thread-safe list
            final MapCallback<String, MappedItem> mapCallback = (file, results) -> mappedItems.addAll(results);

            ExecutorService mapExecutor = Executors.newFixedThreadPool(mapThreadPoolSize);

            for (String file : filesToProcess) {
                String contents = input.get(file);
                mapExecutor.submit(() -> map(file, contents, mapCallback));
            }

            mapExecutor.shutdown();
            try {
                mapExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            // GROUP:
            Map<String, List<String>> groupedItems = new HashMap<>();
            synchronized (mappedItems) {
                for (MappedItem item : mappedItems) {
                    String word = item.getWord();
                    String file = item.getFile();
                    groupedItems.computeIfAbsent(word, k -> new LinkedList<>()).add(file);
                }
            }


            // REDUCE:
            ExecutorService reduceExecutor = Executors.newFixedThreadPool(reduceThreadPoolSize);
            final ReduceCallback<String, String, Integer> reduceCallback = (k, v) -> {
                synchronized(output) {
                    output.put(k, v);
                }
            };

            // Submit reduce tasks to the thread pool
            for (Map.Entry<String, List<String>> entry : groupedItems.entrySet()) {
                String word = entry.getKey();
                List<String> list = entry.getValue();

                reduceExecutor.submit(() -> reduce(word, list, reduceCallback));
            }

            // Shutdown the executor and await termination of reduce tasks
            reduceExecutor.shutdown();
            try {
                reduceExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            long endTimeApproach3 = System.currentTimeMillis();
            System.out.println("Approach #3: Distributed MapReduce took " + (endTimeApproach3 - startTimeApproach3) + " milliseconds.");
            System.out.println(output);
        }
    }

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        // Split the contents into words using a regular expression that matches word characters
        // This regex will split the string into words and exclude punctuation and numbers
        String[] words = contents.trim().split("[^a-zA-Z]+");
        for(String word : words) {
            // Ignore empty strings that might result from split operation
            if (!word.isEmpty()) {
                mappedItems.add(new MappedItem(word, file));
            }
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        // Split the contents into words using a regular expression that matches word characters
        // This regex will split the string into words and exclude punctuation and numbers
        String[] words = contents.trim().split("[^a-zA-Z]+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word : words) {
            // Ignore empty strings that might result from split operation
            if (!word.isEmpty()) {
                results.add(new MappedItem(word, file));
            }
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

    private static String readFile(String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        try {
            if (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine());
            }
            while (scanner.hasNextLine()) {
                fileContents.append(lineSeparator + scanner.nextLine());
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

}