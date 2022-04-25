package db.ExecutionStrategies;

import basicpattern.BPQuadJoinFactory;
import basicpattern.BasicPattern;
import basicpattern.IBPQuadJoin;
import bpiterator.BPFileScan;
import bpiterator.BPSort;
import db.JoinOption;
import db.JoinSortQuery;
import db.Telemetry;
import diskmgr.rdf.RdfDB;
import diskmgr.rdf.TStream;
import global.BPOrder;
import global.QuadOrder;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;

public class QueryExecutionStrategies implements IQueryExecutionStrategies{

  public static JoinSortQuery query;
  public static int numBuf;
  private int basicPatternFieldCount = 0;
  private int strategy;

  public QueryExecutionStrategies(JoinSortQuery query, int numBuf, int strategy){
    this.query = query;
    this.numBuf = numBuf;
    this.strategy = strategy;
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

  public void executeInnerJoin(JoinOption queryJoin1) throws Exception {
    Telemetry.startOperation("initHeapFile");
    Heapfile initialBasicPatternsHeapFile = initHeapFile(true);
    Telemetry.endOperation("initHeapFile");
    if (initialBasicPatternsHeapFile.getRecCnt() > 0) {
      Telemetry.startOperation("Join1");
      //Start First Join
      Heapfile innerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE1");
      BPFileScan initialBasicPatternsFileScan = new BPFileScan("BP_HEAP", 3);
      BasicPattern basicPattern = null;

      IBPQuadJoin innerJoin = BPQuadJoinFactory.createBPQuadJoin(queryJoin1,
          numBuf, 3, initialBasicPatternsFileScan,
          query.getBpJoinNodePosition1(),
          query.getJoinOnSubjectOrObject1(), query.getRightSubjectFilter1(),
          query.getRightPredicateFilter1(), query.getRightObjectFilter1(),
          query.getRightConfidenceFilter1(), query.getLeftOutNodePositions1(),
          query.getOutputRightSubject1(), query.getOutputRightObject1());

      basicPattern = innerJoin.get_next();

      System.out.println("Printing results after first join");
      while (basicPattern != null) {
        basicPattern.printBasicPattern();
        this.basicPatternFieldCount = basicPattern.numberOfFields();
        innerJoinResultsHeapFile.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());
        basicPattern = innerJoin.get_next();
      }
      innerJoin.close();
      initialBasicPatternsFileScan.close();
      initialBasicPatternsHeapFile.deleteFile();
      Telemetry.endOperation("Join1");
    }
  }


  public void executeOuterJoin(JoinOption queryJoin2)
      throws Exception {
      Heapfile outerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE2");
      BPFileScan join1BasicPatternsFileScan = new BPFileScan("JOIN_HEAP_FILE1", basicPatternFieldCount);
      if (basicPatternFieldCount > 0) {
        IBPQuadJoin outerJoin = BPQuadJoinFactory.createBPQuadJoin(queryJoin2,
            numBuf, basicPatternFieldCount, join1BasicPatternsFileScan,
            query.getBpJoinNodePosition2(),
            query.getJoinOnSubjectOrObject2(), query.getRightSubjectFilter2(),
            query.getRightPredicateFilter2(), query.getRightObjectFilter2(),
            query.getRightConfidenceFilter2(), query.getLeftOutNodePositions2(),
            query.getOutputRightSubject2(), query.getOutputRightObject2());

        BasicPattern basicPattern1 = outerJoin.get_next();
        if (basicPattern1 != null) {
          basicPatternFieldCount = basicPattern1.numberOfFields();
        }

        System.out.println("Printing Results after Second Join");
        while (basicPattern1 != null) {
          basicPattern1.printBasicPattern();
          outerJoinResultsHeapFile.insertRecord(basicPattern1.getTupleFromBasicPattern().getTupleByteArray());
          basicPattern1 = outerJoin.get_next();
        }
        outerJoin.close();
      }
      join1BasicPatternsFileScan.close();

      Heapfile innerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE1");
      innerJoinResultsHeapFile.deleteFile();
  }

  public void executeSort()
      throws Exception {
    BPFileScan outerJoinResultsFileScan = null;
    Heapfile outerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE2");
    BasicPattern basicPattern = null;

    if (outerJoinResultsHeapFile.getRecCnt() > 0) {
      outerJoinResultsFileScan = new BPFileScan("JOIN_HEAP_FILE2", basicPatternFieldCount);
      BPSort bpSort = null;
      BPOrder bpOrder = query.getSortOrder();

      bpSort = new BPSort(outerJoinResultsFileScan, bpOrder, query.getSortNodeIDPos(),
          query.getNumberOfPages());

      System.out.println("Printing results after sorting");

      while ((basicPattern = bpSort.get_next()) != null) {
        basicPattern.printBasicPattern();
      }

      bpSort.close();
      outerJoinResultsFileScan.close();
    }

    outerJoinResultsHeapFile.deleteFile();

    System.out.println("Results after strategy execution");
  }

  private JoinOption getInnerJoinOption() {
    if(strategy == 1) {
      return JoinOption.HeapScan;
    } else if(strategy == 2) {
      return JoinOption.IndexScan;
    } else if(strategy == 3) {
      return JoinOption.HeapScan;
    } else if (strategy == 4) {
      return JoinOption.IndexScan;
    } else {
      return JoinOption.HeapScan;
    }

  }

  private JoinOption getOuterJoinOption() {
    if(strategy == 1) {
      return JoinOption.HeapScan;
    } else if(strategy == 2) {
      return JoinOption.HeapScan;
    } else if(strategy == 3) {
      return JoinOption.IndexScan;
    } else if (strategy == 4) {
      return JoinOption.IndexScan;
    } else {
      return JoinOption.HeapScan;
    }
  }
  /**
   * Executing of execution strategies
   * @throws Exception
   */
  public void execute() throws Exception{
    JoinOption innerJoinOption = getInnerJoinOption();
    JoinOption outerJoinOption = getOuterJoinOption();

    Telemetry.initialize();

    executeInnerJoin(innerJoinOption);

      //end of first join


      //start second join
      Telemetry.startOperation("Join2");
      executeOuterJoin(outerJoinOption);

      Telemetry.endOperation("Join2");

      Telemetry.startOperation("Sort");
      executeSort();

      Telemetry.endOperation("Sort");
      Telemetry.printTelemetry();

  }

  @Override
  public void execute(JoinOption queryJoin1, JoinOption queryJoin2) throws Exception {

  }
}
