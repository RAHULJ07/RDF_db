package db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Telemetry {
  public static int readCount;
  public static int writeCount;

  public static int readCountDb;
  public static int writeCountDb;
  public static int operationReadCount;
  public static int operationWriteCount;
  public static Map<String, List<Integer>> operationCountsMap = new HashMap<>();

  /**
   * Initializing read and write count with 0.
   */
  public static void initialize(){
    readCount = 0;
    writeCount = 0;
  }

  /**
   * start operation
   * @param operation
   */
  public static void startOperation(String operation) {
    operationReadCount = readCount;
    operationWriteCount = writeCount;
    List<Integer> countsList = new ArrayList<>();
    countsList.add(0);
    countsList.add(0);
    operationCountsMap.put(operation, countsList);
  }

  /**
   * end operation
   * @param operation
   */
  public static void endOperation(String operation) {
    operationReadCount = readCount - operationReadCount;
    operationWriteCount = writeCount - operationWriteCount;
    List<Integer> countsList = new ArrayList<>();
    countsList.add(operationReadCount);
    countsList.add(operationWriteCount);
    operationCountsMap.put(operation, countsList);
  }



  /**
   * Incrementing read Count.
   */
  public static void readIncrement(){
    readCount++;
    readCountDb++;
  }

  /**
   * Incrementing write count.
   */
  public static void writeIncrement(){
    writeCount++;
    writeCountDb++;
  }

  /**
   * print Telemetry
   */
  public static void printTelemetry(){
    System.out.println("Read Count : " + Telemetry.readCount + " Write Count : " + Telemetry.writeCount);
    for (Map.Entry<String, List<Integer>> set :
        operationCountsMap.entrySet()) {

      // Printing all elements of a Map
      System.out.println(set.getKey() + " = "
          + set.getValue());
    }
  }

  /**
   *  Flush telemetry
   */
  public static void flushTelemetry(){
    readCount = 0;
    readCountDb = 0;
    writeCount = 0;
    writeCountDb = 0;
  }
}
