package cn.fudan.cs.stree.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhsyy
 * @version 1.0
 * @date 2023/6/7 13:59
 */
public class RunningSnapShot {
    static LinkedList<Long> timeRecord = new LinkedList<>();
    static LinkedList<Long> nodeCount = new LinkedList<>();
    static LinkedList<Long> edgeCount = new LinkedList<>();
    static LinkedList<Long> TRDCount = new LinkedList<>();
    static LinkedList<Long> MemUse = new LinkedList<>();

    static long heapSize = 20 * 1024 * 1024;
    public static ConcurrentHashMap<Long, Integer> labeledTs2NodeCount = new ConcurrentHashMap<>();

    public static boolean checkExpirationThreshold(long currentTs, long windowSize, long ratio){
        if (currentTs >= windowSize && ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() > heapSize){
            long total = 0;
            long inWindow = 0;

            for (Integer integer : labeledTs2NodeCount.values()) {
                total += integer;
            }

            for (long i = currentTs; i < currentTs + windowSize; i++) {
                inWindow += labeledTs2NodeCount.getOrDefault(i,0);
            }
            return inWindow * 100 < total * ratio;
        }
        return false;
    }

    public static void showMetric(long currentTs, long windowSize){
        if (!timeRecord.isEmpty()) {
            System.out.println("timeRecord: " + timeRecord.getLast());
            System.out.println("       nodeCount: " + nodeCount.getLast());
            System.out.println("       edgeCount: " + edgeCount.getLast());
            System.out.println("       TRDCount: " + TRDCount.getLast());
            System.out.println("       MemUse: " + MemUse.getLast() / 1024 / 1024);
            double total = 0;
            for (Integer integer : labeledTs2NodeCount.values()) {
                total += integer;
            }
            double inWindow = 0;
            for (long i = currentTs - windowSize; i < currentTs; i++) {
                inWindow += labeledTs2NodeCount.getOrDefault(i,0);
            }
            System.out.println("       total: " + total );
            System.out.println("       Ratio: " + inWindow / total );
        }
    }

    public static void addRecordTime(){
        timeRecord.add(System.currentTimeMillis());
    }
    public static void addNodeCount(long count){
        nodeCount.add(count);
    }

    public static void addEdgeCount(long count){
        edgeCount.add(count);
    }

    public static void addTRDCount(long count){
        TRDCount.add(count);
    }

    public static void addMemUse(long memSize){
        MemUse.add(memSize);
    }

    public static void writeList(){
        try {
            long yourmilliseconds = System.currentTimeMillis();
            SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");
            Date resultdate = new Date(yourmilliseconds);
            BufferedWriter out = new BufferedWriter(new FileWriter("./evaluateExpiration/timeRecord "+sdf.format(resultdate)+".txt", true));
            while (!timeRecord.isEmpty()){
                out.write(timeRecord.removeFirst() + "\n");
            }
            out.close();

            out = new BufferedWriter(new FileWriter("./evaluateExpiration/nodeCount "+sdf.format(resultdate)+".txt", true));
            while (!nodeCount.isEmpty()){
                out.write(nodeCount.removeFirst() + "\n");
            }
            out.close();

            out = new BufferedWriter(new FileWriter("./evaluateExpiration/edgeCount "+sdf.format(resultdate)+".txt", true));
            while (!edgeCount.isEmpty()){
                out.write(edgeCount.removeFirst() + "\n");
            }
            out.close();

            out = new BufferedWriter(new FileWriter("./evaluateExpiration/TRDCount "+sdf.format(resultdate)+".txt", true));
            while (!TRDCount.isEmpty()){
                out.write(TRDCount.removeFirst() + "\n");
            }
            out.close();

            out = new BufferedWriter(new FileWriter("./evaluateExpiration/MemUse "+sdf.format(resultdate)+".txt", true));
            while (!MemUse.isEmpty()){
                out.write(MemUse.removeFirst() + "\n");
            }
            out.close();
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
