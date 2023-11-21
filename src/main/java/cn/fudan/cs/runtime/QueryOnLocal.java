package cn.fudan.cs.runtime;

import cn.fudan.cs.input.InputTuple;
import cn.fudan.cs.input.SimpleTextStreamWithExplicitDeletions;
import cn.fudan.cs.input.TextFileStream;
import cn.fudan.cs.stree.query.ManualQueryAutomata;
import cn.fudan.cs.stree.data.arbitrary.TRDRAPQ;
import cn.fudan.cs.stree.data.arbitrary.TRDNodeRAPQ;
import cn.fudan.cs.stree.engine.RPQEngine;
import cn.fudan.cs.stree.engine.WindowedRPQ;
import cn.fudan.cs.stree.util.RunningSnapShot;
import cn.fudan.cs.stree.util.Semantics;
import org.apache.commons.cli.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * @author zhsyy
 * @version 1.0
 * @date 2023/1/7 8:59
 */

public class QueryOnLocal {
    private static Logger logger = LoggerFactory.getLogger(QueryOnLocal.class);

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(getCLIOptions(), args);
        } catch (ParseException e) {
            logger.error("Command line argument can NOT be parsed", e);
            return;
        }

        // -f sample -fp ./src/resources/ -q 3 -ms 2000000 -ws 30 -ss 7 -tc 5 -gt 50
        // parse all necessary command line options
        String filePath = line.getOptionValue("fp", "./src/main/resources/");
        String fileName = line.getOptionValue("f", "sample") + ".txt";
        String queryCase = line.getOptionValue("q", "q_3");
        int maxSize = Integer.parseInt(line.getOptionValue("ms", "200000"));
        long windowSize = Long.parseLong(line.getOptionValue("ws", "30"));
        long slideSize = Long.parseLong(line.getOptionValue("ss", "7"));
        int threadCount = Integer.parseInt(line.getOptionValue("tc", "5"));
        int GCThreshold = Integer.parseInt(line.getOptionValue("gt", "50"));
        int fixedThroughput = Objects.equals(line.getOptionValue("ft"), "") ? Integer.parseInt(line.getOptionValue("ft")):Integer.MAX_VALUE;

        ManualQueryAutomata<String> query = ManualQueryAutomata.getManualQuery(queryCase);

        RPQEngine<String> rapqEngine = new WindowedRPQ<String, TRDRAPQ<Integer>, TRDNodeRAPQ<Integer>>
                (query, 10000, windowSize, slideSize, threadCount, Semantics.ARBITRARY);
        Queue<InputTuple<Integer, Integer, String>> cacheQueue = new ConcurrentLinkedQueue<>();


        TextFileStream<Integer, Integer, String> stream = new SimpleTextStreamWithExplicitDeletions();
        stream.open(filePath+fileName, maxSize);

        Multimap<Long, Long> labeledTs2AllClockTs = HashMultimap.create();
        ScheduledExecutorService cacheQueueThread = Executors.newScheduledThreadPool(1);
        Runnable readTuples = () -> {
            InputTuple<Integer, Integer, String> input;
            for (int i = 0; i < fixedThroughput; i++) {
                input = stream.next();
                if (input != null) {
                    cacheQueue.add(input);
                    labeledTs2AllClockTs.put(input.getTimestamp(), System.currentTimeMillis());
                } else {
                    logger.info("read finish!");
                    cacheQueueThread.shutdown();
                    break;
                }
            }
        };
        cacheQueueThread.scheduleAtFixedRate(readTuples, 0, 1, TimeUnit.SECONDS);
        if (!line.hasOption("tt")) {
            while (!cacheQueueThread.isShutdown()) {
            }
        }

        InputTuple<Integer, Integer, String> input;
        long begin = System.currentTimeMillis();
        long lastGCTimestamp = windowSize - slideSize;
        long totalProcessedTupleCount = 0;

        while (!cacheQueue.isEmpty() || !cacheQueueThread.isShutdown()) {
            input = cacheQueue.poll();
            try {
                if (input != null) {
                    long ts = input.getTimestamp();
                    if (ts > lastGCTimestamp
                            && RunningSnapShot.checkExpirationThreshold(ts, windowSize, GCThreshold)) {
                        lastGCTimestamp = ts;
                        rapqEngine.GC(input.getTimestamp());
                    }
                    totalProcessedTupleCount++;
                    rapqEngine.processEdge(input);
                }
            } catch (OutOfMemoryError | ExecutionException | InterruptedException e) {
                logger.info("GC when no enough memory");
                rapqEngine.GC(input.getTimestamp());
            }
        }
        rapqEngine.shutDown();
        double totalTime = (System.currentTimeMillis() - begin) * 1.0 /1000;
        stream.close();
        rapqEngine.getResults().reportResult();
        logger.info("AvgThroughput = " + totalProcessedTupleCount / totalTime);
    }

    private static Options getCLIOptions() {
        Options options = new Options();
        options.addOption("f", "file", true, "Text file to read");
        options.addOption("fp", "file-path", true, "Directory to store datasets");
        options.addOption("q", "query-case", true, "Query case");
        options.addOption("ms", "max-size", true, "Maximum size to be processed");
        options.addOption("ws", "window-size", true, "Size of the window");
        options.addOption("ss", "slide-size", true, "Slide of the window");
        options.addOption("tc", "threadCount", true, "# of Threads for inter-query parallelism");
        options.addOption("gt", "garbage-collection-threshold", true, "Threshold to execute DGC");
        options.addOption("ft", "fixed-throughput", true, "Fixed fetch rate from dataset");
        options.addOption("tt", "file", false, "Test throughput");
        return options;
    }
}