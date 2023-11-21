package cn.fudan.cs.stree.data;

import cn.fudan.cs.stree.util.Constants;
import cn.fudan.cs.stree.util.Hasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.*;

public class ResultPair implements Serializable, Comparable {
    private static final Logger logger = LoggerFactory.getLogger(ResultPair.class);

    private final ExecutorService recordResultsThread;

    private ConcurrentHashMap<Hasher.MapKey<Integer>, HashMap<Long, Long>> result2labeledTs_clockTs;
    public ResultPair() {
        recordResultsThread = Executors.newFixedThreadPool(1);
        result2labeledTs_clockTs = new ConcurrentHashMap<>(Constants.HISTOGRAM_BUCKET_SIZE);
    }

    public void addNewResult(final int source, final int target, AbstractTRDNode node){
        Runnable worker = new WorkerThread(source, target, node);
        recordResultsThread.submit(worker);
    }
    public int getResultSize(){
        return result2labeledTs_clockTs.size();
    }

    public ConcurrentHashMap<Hasher.MapKey<Integer>, HashMap<Long, Long>> getResult(){
        return result2labeledTs_clockTs;
    }

    public void reportResult(){
        for (Hasher.MapKey<Integer> key :
                result2labeledTs_clockTs.keySet()) {
            String resultKey = "<" + key.X + ", " + key.Y + ">";
            StringBuilder timestamps = new StringBuilder();
            for (long ts :
                    result2labeledTs_clockTs.get(key).keySet()) {
                timestamps.append(ts).append(" ");
            }
            logger.info(resultKey + ", time : " + timestamps);
        }
    }
    @Override
    public int compareTo(Object o) {
        return 0;
    }

    public void shutDown() {
        recordResultsThread.shutdown();
        while (!recordResultsThread.isTerminated()) {
        }
    }

    class WorkerThread implements Runnable {
        private final int source;
        private final int target;
        AbstractTRDNode node;

        public WorkerThread(final int source, final int target, AbstractTRDNode node) {
            this.source = source;
            this.target = target;
            this.node = node;
        }

        @Override
        public void run() {
            node.lock.readLock().lock();
            LinkedList<Long> lower_ts = node.lower_bound_timestamp;
            LinkedList<Long> upper_ts = node.upper_bound_timestamp;
            if (result2labeledTs_clockTs.containsKey(Hasher.getThreadLocalTreeNodePairKey(source, target))){
                HashMap<Long, Long> labeledTs2ClockTs = result2labeledTs_clockTs.get(Hasher.getThreadLocalTreeNodePairKey(source, target));
                Iterator<Long> iterator_low = lower_ts.iterator();
                Iterator<Long> iterator_high = upper_ts.iterator();
                while (iterator_low.hasNext()){
                    long low = iterator_low.next();
                    long high = iterator_high.next();
                    for (long i = low; i <= high; i++) {
                        labeledTs2ClockTs.putIfAbsent(i, System.currentTimeMillis());
                    }
                }
            } else {
                HashMap<Long, Long> labeledTs2ClockTs = new HashMap<>();
                Iterator<Long> iterator_low = lower_ts.iterator();
                Iterator<Long> iterator_high = upper_ts.iterator();
                while (iterator_low.hasNext()){
                    long low = iterator_low.next();
                    long high = iterator_high.next();
                    for (long i = low; i <= high; i++) {
                        labeledTs2ClockTs.put(i, System.currentTimeMillis());
                    }
                }
                result2labeledTs_clockTs.put(Hasher.createTreeNodePairKey(source, target), labeledTs2ClockTs);
            }
            node.lock.readLock().unlock();
        }
    }
}
