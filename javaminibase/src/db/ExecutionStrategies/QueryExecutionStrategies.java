package db.ExecutionStrategies;

import basicpattern.BPQuadHeapJoin;
import basicpattern.BPQuadIndexJoin;
import basicpattern.BPQuadJoinFactory;
import basicpattern.BasicPattern;
import basicpattern.IBPQuadJoin;
import bpiterator.BPFileScan;
import bpiterator.BPSort;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import db.InnerJoinOption;
import db.JoinSortQuery;
import db.Telemetry;
import diskmgr.rdf.RdfDB;
import diskmgr.rdf.TStream;
import global.BPOrder;
import global.QuadOrder;
import global.RDFSystemDefs;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.TupleUtilsException;
import java.io.IOException;

public class QueryExecutionStrategies implements IQueryExecutionStrategies{

  public static JoinSortQuery query;
  public static int numBuf;

  public QueryExecutionStrategies(JoinSortQuery query, int numBuf){
    this.query = query;
    this.numBuf = numBuf;
  }

  private Heapfile initHeapFile(boolean printBasicPattern)
      throws Exception {
    Heapfile heapfile = new Heapfile("BP_HEAP");
    try{
      QuadOrder quadOrder = new QuadOrder(7);
      TStream stream = ((RdfDB) SystemDefs.JavabaseDB).openTStream(quadOrder, numBuf,
          query.getSubjectFilter1(), query.getPredicateFilter1(), query.getObjectFilter1(),
          query.getConfidenceFilter1());

      BasicPattern basicPattern = null;
      Tuple tupleOfBasicPattern = null;

      if(printBasicPattern){
        System.out.println("Printing  Basic Patterns from BP HeapFile");
      }

      while ((basicPattern = stream.getNextBasicPatternFromQuadruple()) != null) {
        if(printBasicPattern){
          basicPattern.printBasicPattern();
        }
        tupleOfBasicPattern = basicPattern.getTupleFromBasicPattern();
        heapfile.insertRecord(tupleOfBasicPattern.getTupleByteArray());
      }

      if (stream != null) {
        stream.closeStream();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return heapfile;
  }

  /**
   * Executing of execution strategies
   * @param queryJoin1
   * @param queryJoin2
   * @throws Exception
   */
  @Override
  public void execute(InnerJoinOption queryJoin1, InnerJoinOption queryJoin2) throws Exception{
    int fieldCount1 = 0;
    int fieldCount2 = 0;

    Heapfile heapfile = initHeapFile(true);

    if (heapfile.getRecCnt() > 0) {

      //Start First Join
      Heapfile joinHeapFile1 = new Heapfile("JOIN_HEAP_FILE1");
      BPFileScan bpFileScan = new BPFileScan("BP_HEAP", 3);
      BasicPattern basicPattern = null;

      IBPQuadJoin bpQuadJoin = BPQuadJoinFactory.createBPQuadJoin(queryJoin1 ,
          numBuf, 3, bpFileScan,
          query.getBpJoinNodePosition1(),
          query.getJoinOnSubjectOrObject1(), query.getRightSubjectFilter1(),
          query.getRightPredicateFilter1(), query.getRightObjectFilter1(),
          query.getRightConfidenceFilter1(), query.getLeftOutNodePositions1(),
          query.getOutputRightSubject1(), query.getOutputRightObject1());

      basicPattern = bpQuadJoin.get_next();


      System.out.println("Printing results after first join");
      while (basicPattern != null) {
        basicPattern.printBasicPattern();
        fieldCount1 = basicPattern.numberOfFields();
        joinHeapFile1.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());
        basicPattern = bpQuadJoin.get_next();
      }
      bpQuadJoin.close();
      //end of first join

      //start second join

      Heapfile joinHeapFile2 = new Heapfile("JOIN_HEAP_FILE2");
      if (fieldCount1 > 0) {
        bpFileScan = new BPFileScan("JOIN_HEAP_FILE1", fieldCount1);
        bpQuadJoin = BPQuadJoinFactory.createBPQuadJoin(queryJoin2 ,
            numBuf, fieldCount1, bpFileScan,
            query.getBpJoinNodePosition2(),
            query.getJoinOnSubjectOrObject2(), query.getRightSubjectFilter2(),
            query.getRightPredicateFilter2(), query.getRightObjectFilter2(),
            query.getRightConfidenceFilter2(), query.getLeftOutNodePositions2(),
            query.getOutputRightSubject2(), query.getOutputRightObject2());

        BasicPattern basicPattern1 = bpQuadJoin.get_next();
        if (basicPattern1 != null) {
          fieldCount2 = basicPattern1.numberOfFields();
        }

        System.out.println("Printing Results after Second Join");
        while (basicPattern1 != null) {
          basicPattern1.printBasicPattern();
          joinHeapFile2.insertRecord(basicPattern1.getTupleFromBasicPattern().getTupleByteArray());
          basicPattern1 = bpQuadJoin.get_next();
        }
        bpQuadJoin.close();
      }

      joinHeapFile1.deleteFile();

      try {
        if (joinHeapFile2.getRecCnt() > 0) {

          BPFileScan bpFileScan2 = null;

          bpFileScan2 = new BPFileScan("JOIN_HEAP_FILE2", fieldCount2);
          BPSort bpSort = null;
          BPOrder bpOrder = query.getSortOrder();

          bpSort = new BPSort(bpFileScan2, bpOrder, query.getSortNodeIDPos(),
              query.getNumberOfPages());

          System.out.println("Printing results after sorting");

          while ((basicPattern = bpSort.get_next()) != null) {
            basicPattern.printBasicPattern();
          }

          bpSort.close();
        }
        joinHeapFile2.deleteFile();

        System.out.println("Results after strategy execution");
        Telemetry.printTelemetry();
        Telemetry.initialize();
        heapfile.deleteFile();
        RDFSystemDefs.forceFlushBuffer();

      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      System.out.println(
          "Correct Query Execution order of Parametres : RDFDBNAME QUERYFILE NUMBUF");
      return;
    }
  }

}
