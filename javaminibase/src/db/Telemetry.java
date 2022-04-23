package db;

public class Telemetry {
  public static int readCount;
  public static int writeCount;

  public static int readCountDb;
  public static int writeCountDb;

  /**
   * Initializing read and write count with 0.
   */
  public static void initialize(){
    readCount = 0;
    writeCount = 0;
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
