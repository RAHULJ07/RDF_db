package programs.phase2;

import db.Telemetry;
import diskmgr.rdf.RdfDB;
import global.RDFSystemDefs;

public class Report {
  public static void main(String[] args) {
    Telemetry.printAllTelemetry();
  }
}
