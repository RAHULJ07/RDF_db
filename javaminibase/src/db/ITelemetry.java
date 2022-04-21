package db;

public interface ITelemetry {

  /**
   * Initializes telemetry class
   * by reading the already existing file
   * if it exits, otherwise by creating the file.
   */
  void initialize();

}
