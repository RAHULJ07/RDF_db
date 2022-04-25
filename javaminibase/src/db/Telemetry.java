package db;

import diskmgr.rdf.RdfDB;
import global.RDFSystemDefs;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import global.GlobalConst;
import java.util.Iterator;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Telemetry {
  public static int readCount;
  public static int writeCount;

  public static int readCountDb;
  public static int writeCountDb;

  public static int operationReadCount;
  public static int operationWriteCount;
  public static Map<String, List<Integer>> operationCountsMap = new HashMap<>();

  private String dbName;

  public Telemetry(String _dbName) {
    readCountDb = 0;
    writeCountDb = 0;
    dbName = _dbName;
  }

  /**
   * Initializing read and write count with 0.
   */
  public static void initialize(){
    readCount = 0;
    writeCount = 0;
  }

  /**
   * get telemetry
   * @return
   */
  private static JSONObject getTelemetry() {
    JSONObject dbJsonObject = new JSONObject();
    try {
      File telemetryFile = new File(GlobalConst.JSON_FILE);
      if (telemetryFile.exists()) {
        JSONParser jsonParser = new JSONParser();
        dbJsonObject = (JSONObject) jsonParser.parse(
            new FileReader(GlobalConst.JSON_FILE));
      }
    } catch (Exception e) {
      // catch all json parsing exception
      e.printStackTrace();
    }
    return dbJsonObject;
  }

  /**
   * put Telemetry
   * @param dbJsonObject
   */
  private void putTelemetry(JSONObject dbJsonObject) {
    File telemetryFile = new File(GlobalConst.JSON_FILE);
    try (FileWriter file = new FileWriter(telemetryFile)) {
      file.write(dbJsonObject.toJSONString());
      file.flush();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Flushes the telemetry updates to the file.
   */
  public void flush() throws IOException, ParseException {
    JSONObject dbJsonObject = getTelemetry();
    JSONObject dataObject = new JSONObject();
    if (dbJsonObject.containsKey(dbName)) {
      dataObject = (JSONObject) dbJsonObject.get(dbName);
    }
    int rCount = 0, wCount = 0, subCount = 0, objCount = 0, predCount = 0, entCount = 0, quadCount = 0;
    if (dataObject.containsKey(GlobalConst.READS)) {
      rCount = Integer.parseInt(dataObject.get(GlobalConst.READS).toString());
    }
    dataObject.put(GlobalConst.READS, rCount + readCountDb);

    if (dataObject.containsKey(GlobalConst.WRITES)) {
      wCount = Integer.parseInt(dataObject.get(GlobalConst.WRITES).toString());
    }
    dataObject.put(GlobalConst.WRITES, wCount + writeCountDb);

    dataObject.put(GlobalConst.SUBJECT_COUNT, ((RdfDB) RDFSystemDefs.JavabaseDB).getSubjectCount());

    dataObject.put(GlobalConst.OBJECT_COUNT, ((RdfDB)RDFSystemDefs.JavabaseDB).getObjectCount());

    dataObject.put(GlobalConst.PREDICATE_COUNT, ((RdfDB)RDFSystemDefs.JavabaseDB).getPredicateCount());

    dataObject.put(GlobalConst.ENTITY_COUNT, ((RdfDB)RDFSystemDefs.JavabaseDB).getEntityCount());

    dataObject.put(GlobalConst.QUAD_COUNT, ((RdfDB)RDFSystemDefs.JavabaseDB).getQuadrupleCount());

    dbJsonObject.put(dbName, dataObject);
    putTelemetry(dbJsonObject);
  }

  /**
   * Print all telemetry for report
   */
  public static void printAllTelemetry() {
    JSONObject telemetry  = getTelemetry();
    for (Iterator iterator = telemetry.keySet().iterator(); iterator.hasNext(); ) {
      String dbName = (String) iterator.next();
      JSONObject dataObject = (JSONObject) telemetry.get(dbName);
      int rCount = 0, wCount = 0, subCount = 0, objCount = 0, predCount = 0, entCount = 0, quadCount = 0;;
      if (dataObject.containsKey(GlobalConst.READS)) {
        rCount = Integer.parseInt(dataObject.get(GlobalConst.READS).toString());
      }
      if (dataObject.containsKey(GlobalConst.WRITES)) {
        wCount = Integer.parseInt(dataObject.get(GlobalConst.WRITES).toString());
      }
      if (dataObject.containsKey(GlobalConst.SUBJECT_COUNT)) {
        subCount = Integer.parseInt(dataObject.get(GlobalConst.SUBJECT_COUNT).toString());
      }

      if (dataObject.containsKey(GlobalConst.OBJECT_COUNT)) {
        objCount = Integer.parseInt(dataObject.get(GlobalConst.OBJECT_COUNT).toString());
      }

      if (dataObject.containsKey(GlobalConst.PREDICATE_COUNT)) {
        predCount = Integer.parseInt(dataObject.get(GlobalConst.PREDICATE_COUNT).toString());
      }

      if (dataObject.containsKey(GlobalConst.ENTITY_COUNT)) {
        entCount = Integer.parseInt(dataObject.get(GlobalConst.ENTITY_COUNT).toString());
      }

      if (dataObject.containsKey(GlobalConst.QUAD_COUNT)) {
        quadCount = Integer.parseInt(dataObject.get(GlobalConst.QUAD_COUNT).toString());
      }

      System.out.println("DB Name: " + dbName + " Reads: " + rCount + " Writes: " + wCount +
                         " Subject Count: " + subCount + " Object Count: " + objCount +
                         " Predicate Count: " + predCount + " Entity Count: " + entCount +
                         " Quadruple Count: " + quadCount);
    }
  }

  /**
   * Returns the read counter.
   *
   */
  public int getRead() { return readCountDb; }

  /**
   * Returns the write counter.
   *
   */
  public int getWrite() { return writeCountDb; }

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
