package programs.phase2;

import db.Telemetry;
import diskmgr.rdf.RdfDB;
import global.RDFSystemDefs;

public class Report {
  public static void main(String[] args) {

    int readCount = Telemetry.readCountDb;
    int writeCount =  Telemetry.writeCountDb;
    int subjectCount = ((RdfDB)RDFSystemDefs.JavabaseDB).getSubjectCount();
    int objectCount = ((RdfDB)RDFSystemDefs.JavabaseDB).getObjectCount();
    int predicateCount = ((RdfDB)RDFSystemDefs.JavabaseDB).getPredicateCount();

    System.out.println("Read Count : " + readCount + " Write Count : " + writeCount +
        "Subject Count : " + subjectCount + " Object Count : " + objectCount +
        "Predicate count : " + predicateCount);

  }
}
