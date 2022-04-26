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
    Heapfile initialBasicPatternsHeapFile = new Heapfile("BP_HEAP");
    // if there were any basic patterns retrieved

      Heapfile innerJoinResultsHeapFile;
      IBPQuadJoin innerJoin = null;
      BPFileScan initialBasicPatternsFileScan = null;

      try {
        innerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE1");
        initialBasicPatternsFileScan = new BPFileScan("BP_HEAP", 3);
        BasicPattern basicPattern = null;

        innerJoin = BPQuadJoinFactory.createBPQuadJoin(queryJoin1,
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

      } catch(Exception e) {
        System.out.println("Error in inner join.");
        throw e;
      } finally {
         if(innerJoin != null) {
           innerJoin.close();
         }
         if(initialBasicPatternsFileScan != null) {
           initialBasicPatternsFileScan.close();
         }
         if(initialBasicPatternsHeapFile != null) {
           initialBasicPatternsHeapFile.deleteFile();
         }
      }
  }

  public void executeOuterJoin(JoinOption queryJoin2)
      throws Exception {
    Heapfile innerJoinResultsHeapFile = null;
    IBPQuadJoin outerJoin = null;
    BPFileScan join1BasicPatternsFileScan = null;
    Heapfile outerJoinResultsHeapFile = null;
    try {
      innerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE1");
      outerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE2");

      if (basicPatternFieldCount > 0) {
        join1BasicPatternsFileScan = new BPFileScan("JOIN_HEAP_FILE1", basicPatternFieldCount);
        outerJoin = BPQuadJoinFactory.createBPQuadJoin(queryJoin2,
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
      }

    } catch(Exception e) {
      System.out.println("Error in outer join.");
      throw e;
    } finally {
      if(outerJoin != null) {
        outerJoin.close();
      }
      if(join1BasicPatternsFileScan != null) {
        join1BasicPatternsFileScan.close();
      }
      if(innerJoinResultsHeapFile != null) {
        innerJoinResultsHeapFile.deleteFile();
      }
      if(outerJoinResultsHeapFile != null) {

      }
    }
  }

  public void executeSort()
      throws Exception {
    BPFileScan outerJoinResultsFileScan = null;
    Heapfile outerJoinResultsHeapFile = null;
    BasicPattern basicPattern = null;
    BPSort bpSort = null;

    try {

      outerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE2");


      if (outerJoinResultsHeapFile.getRecCnt() > 0) {
        outerJoinResultsFileScan = new BPFileScan("JOIN_HEAP_FILE2", basicPatternFieldCount);

        BPOrder bpOrder = query.getSortOrder();
        bpSort = new BPSort(outerJoinResultsFileScan, bpOrder, query.getSortNodeIDPos(),
            query.getNumberOfPages());

        System.out.println("Printing results after sorting");

        while ((basicPattern = bpSort.get_next()) != null) {
          basicPattern.printBasicPattern();
        }
      }

    } catch(Exception e) {
        System.out.println("Error in sort");
        throw e;
    } finally {
      if(bpSort != null) {
        bpSort.close();
      }
      if(outerJoinResultsFileScan != null) {
        outerJoinResultsFileScan.close();
      }
      if(outerJoinResultsHeapFile != null) {
        outerJoinResultsHeapFile.deleteFile();
      }
    }


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
    try {
      JoinOption innerJoinOption = getInnerJoinOption();
      JoinOption outerJoinOption = getOuterJoinOption();

      Telemetry.initialize();

      Telemetry.startOperation("initHeapFile");
      initHeapFile(true);
      Telemetry.endOperation("initHeapFile");

      Telemetry.startOperation("Join1");
      if(canExecuteInnerJoin()) {
        executeInnerJoin(innerJoinOption);
      }

      Telemetry.endOperation("Join1");

      Telemetry.startOperation("Join2");
      if(canExecuteOuterJoin()) {
        executeOuterJoin(outerJoinOption);
      }
      Telemetry.endOperation("Join2");


      Telemetry.startOperation("Sort");
      if(canExecuteSort()) {
        executeSort();
      }
      Telemetry.endOperation("Sort");

      Telemetry.prinTelemetry();



    } catch(Exception e) {
      System.out.println("Error executing the query.");
      throw e;
    }
  }

  private boolean canExecuteInnerJoin() throws Exception {
    Heapfile initialBasicPatternsHeapFile = new Heapfile("BP_HEAP");
    return initialBasicPatternsHeapFile.getRecCnt() > 0;
  }

  private boolean canExecuteSort() throws Exception {
    Heapfile outerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE2");
    return ((basicPatternFieldCount > 0) && (outerJoinResultsHeapFile.getRecCnt() > 0));
  }

  private boolean canExecuteOuterJoin()
      throws Exception {
    Heapfile innerJoinResultsHeapFile = new Heapfile("JOIN_HEAP_FILE1");
    return ((basicPatternFieldCount > 0) && (innerJoinResultsHeapFile.getRecCnt() > 0));
  }

  @Override
  public void execute(JoinOption queryJoin1, JoinOption queryJoin2) throws Exception {

  }
}
