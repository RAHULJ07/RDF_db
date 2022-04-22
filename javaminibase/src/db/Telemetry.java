package db;

public class Telemetry {
  public static int readCount;
  public static int writeCount;


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
  }

  /**
   * Incrementing write count.
   */
  public static void writeIncrement(){
    writeCount++;
  }

  public  static void prinTelemetry(){
    System.out.println("Read Count : " + Telemetry.readCount + " Write Count : " + Telemetry.writeCount);
  }
}
