package db.ExecutionStrategies;

import basicpattern.BPQuadHeapJoin;
import basicpattern.BPQuadIndexJoin;
import basicpattern.BasicPattern;
import bpiterator.BPFileScan;
import bpiterator.BPSort;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
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

  public static Heapfile initHeapFile(boolean printBasicPattern)
      throws HFDiskMgrException, HFException, HFBufMgrException, IOException {
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

    } catch (SpaceNotAvailableException e) {
      e.printStackTrace();
    } catch (HFDiskMgrException e) {
      e.printStackTrace();
    } catch (HFException e) {
      e.printStackTrace();
    } catch (HFBufMgrException e) {
      e.printStackTrace();
    } catch (InvalidSlotNumberException e) {
      e.printStackTrace();
    } catch (InvalidTupleSizeException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return heapfile;
  }


  @Override
  public void executeHeapfileHeapfileStrategy() throws Exception {

    int fieldCount1 = 0;
    int fieldCount2 = 0;

    Heapfile heapfile = initHeapFile(true);

    if (heapfile.getRecCnt() > 0) {

      //Start First Join
      Heapfile joinHeapFile1 = new Heapfile("JOIN_HEAP_FILE1");
      BPFileScan bpFileScan = new BPFileScan("BP_HEAP", 3);
      BasicPattern basicPattern = null;

      BPQuadHeapJoin bpQuadHeapJoin = new BPQuadHeapJoin(numBuf, 3, bpFileScan,
          query.getBpJoinNodePosition1(),
          query.getJoinOnSubjectOrObject1(), query.getRightSubjectFilter1(),
          query.getRightPredicateFilter1(), query.getRightObjectFilter1(),
          query.getRightConfidenceFilter1(), query.getLeftOutNodePositions1(),
          query.getOutputRightSubject1(), query.getOutputRightObject1());

      basicPattern = bpQuadHeapJoin.get_next();


      System.out.println("Printing results after first join in 1.HeapfileHeapfileStrategy: ");
      while (basicPattern != null) {
        basicPattern.printBasicPattern();
        fieldCount1 = basicPattern.numberOfFields();
        joinHeapFile1.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());
        basicPattern = bpQuadHeapJoin.get_next();
      }
      bpQuadHeapJoin.close();
      //end of first join

      //start second join

      Heapfile joinHeapFile2 = new Heapfile("JOIN_HEAP_FILE2");
      if (fieldCount1 > 0) {
        bpFileScan = new BPFileScan("JOIN_HEAP_FILE1", fieldCount1);
        bpQuadHeapJoin = new BPQuadHeapJoin(numBuf, fieldCount1, bpFileScan,
            query.getBpJoinNodePosition2(),
            query.getJoinOnSubjectOrObject2(), query.getRightSubjectFilter2(),
            query.getRightPredicateFilter2(), query.getRightObjectFilter2(),
            query.getRightConfidenceFilter2(), query.getLeftOutNodePositions2(),
            query.getOutputRightSubject2(), query.getOutputRightObject2());

        BasicPattern basicPattern1 = bpQuadHeapJoin.get_next();
        if (basicPattern1 != null) {
          fieldCount2 = basicPattern1.numberOfFields();
        }

        System.out.println("Printing Results after Second Join in 1.HeapfileHeapfileStrategy");
        while (basicPattern1 != null) {
          basicPattern1.printBasicPattern();
          joinHeapFile2.insertRecord(basicPattern1.getTupleFromBasicPattern().getTupleByteArray());
          basicPattern1 = bpQuadHeapJoin.get_next();
        }
        bpQuadHeapJoin.close();
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

          System.out.println("Printing results after sorting in 1.HeapfileHeapfileStrategy: ");

          while ((basicPattern = bpSort.get_next()) != null) {
            basicPattern.printBasicPattern();
          }

          bpSort.close();
        }
        joinHeapFile2.deleteFile();

        System.out.println("After 1.HeapfileHeapfileStrategy Strategy : ");
        Telemetry.prinTelemetry();
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

    //End Strategy 1

  }

  @Override
  public void executeIndexHeapfileStrategy() {

    try{
      Heapfile heapfile = initHeapFile(false);

      int fieldCount1 = 0;
      int fieldCount2 = 0;

      //Start Strategy 2 , This join does QuadIndexJoin on the first Join and QuadHeapFileJoin on

      if (heapfile.getRecCnt() > 0) {

        //Start First Join
        Heapfile joinHeapFile1 = new Heapfile("JOIN_HEAP_FILE1");
        BPFileScan bpFileScan = new BPFileScan("BP_HEAP", 3);
        BasicPattern basicPattern = null;
        BPQuadIndexJoin bpQuadIndexJoin = new BPQuadIndexJoin(numBuf, 3, bpFileScan,
            query.getBpJoinNodePosition1(),
            query.getJoinOnSubjectOrObject1(), query.getRightSubjectFilter1(),
            query.getRightPredicateFilter1(), query.getRightObjectFilter1(),
            query.getRightConfidenceFilter1(), query.getLeftOutNodePositions1(),
            query.getOutputRightSubject1(), query.getOutputRightObject1());

        basicPattern = bpQuadIndexJoin.get_next();
        if (basicPattern != null) {
          fieldCount1 = basicPattern.numberOfFields();
        }

        System.out.println("Printing results after first join in 2.IndexHeapfileStrategy: ");
        while (basicPattern != null) {
          basicPattern.printBasicPattern();
          joinHeapFile1.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());
          basicPattern = bpQuadIndexJoin.get_next();
        }
        bpQuadIndexJoin.close();


        //end of first join
        //start second join

        Heapfile joinHeapFile2 = new Heapfile("JOIN_HEAP_FILE2");
        if (fieldCount1 > 0) {
          bpFileScan = new BPFileScan("JOIN_HEAP_FILE1", fieldCount1);
          BPQuadHeapJoin bpQuadHeapJoin = new BPQuadHeapJoin(numBuf, fieldCount1, bpFileScan,
              query.getBpJoinNodePosition2(),
              query.getJoinOnSubjectOrObject2(), query.getRightSubjectFilter2(),
              query.getRightPredicateFilter2(), query.getRightObjectFilter2(),
              query.getRightConfidenceFilter2(), query.getLeftOutNodePositions2(),
              query.getOutputRightSubject2(), query.getOutputRightObject2());

          BasicPattern basicPattern1 = bpQuadHeapJoin.get_next();

          if (basicPattern1 != null) {
            fieldCount2 = basicPattern1.numberOfFields();
          }

          System.out.println("Printing Results after Second Join in 2.IndexHeapfileStrategy");
          while (basicPattern1 != null) {
            basicPattern1.printBasicPattern();
            joinHeapFile2.insertRecord(basicPattern1.getTupleFromBasicPattern().getTupleByteArray());
            basicPattern1 = bpQuadHeapJoin.get_next();
          }
          bpQuadHeapJoin.close();
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

            System.out.println("Printing results after sorting in 2.IndexHeapfileStrategy : ");

            while ((basicPattern = bpSort.get_next()) != null) {
              basicPattern.printBasicPattern();
            }

            bpSort.close();
          }
          joinHeapFile2.deleteFile();

          System.out.println("After 2.IndexHeapfileStrategy Strategy : ");
          Telemetry.prinTelemetry();
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

      //End Strategy 2
    } catch (InvalidRelation e) {
      e.printStackTrace();
    } catch (HashOperationException e) {
      e.printStackTrace();
    } catch (BufMgrException e) {
      e.printStackTrace();
    } catch (FileScanException e) {
      e.printStackTrace();
    } catch (InvalidSlotNumberException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SpaceNotAvailableException e) {
      e.printStackTrace();
    } catch (FieldNumberOutOfBoundException e) {
      e.printStackTrace();
    } catch (PageNotFoundException e) {
      e.printStackTrace();
    } catch (HFDiskMgrException e) {
      e.printStackTrace();
    } catch (HFException e) {
      e.printStackTrace();
    } catch (HFBufMgrException e) {
      e.printStackTrace();
    } catch (InvalidTupleSizeException e) {
      e.printStackTrace();
    } catch (TupleUtilsException e) {
      e.printStackTrace();
    } catch (InvalidTypeException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  public void executeHeapfileIndexStrategy() {

    try{
      int fieldCount1 = 0;
      int fieldCount2 = 0;

      //Start Strategy3, This Join does IndexJoin on the second Join and HeapFileJoin on first Join.
      Heapfile heapfile = initHeapFile(false);
      if (heapfile.getRecCnt() > 0) {

        //Start First Join
        Heapfile joinHeapFile1 = new Heapfile("JOIN_HEAP_FILE1");
        BPFileScan bpFileScan = new BPFileScan("BP_HEAP", 3);

        BPQuadHeapJoin bpQuadHeapJoin = new BPQuadHeapJoin(numBuf, 3, bpFileScan,
            query.getBpJoinNodePosition1(),
            query.getJoinOnSubjectOrObject1(), query.getRightSubjectFilter1(),
            query.getRightPredicateFilter1(), query.getRightObjectFilter1(),
            query.getRightConfidenceFilter1(), query.getLeftOutNodePositions1(),
            query.getOutputRightSubject1(), query.getOutputRightObject1());
        BasicPattern basicPattern = null;
        basicPattern = bpQuadHeapJoin.get_next();

        if (basicPattern != null) {
          fieldCount1 = basicPattern.numberOfFields();
        }

        System.out.println("Printing results after first join in 3.HeapfileIndexStrategy");
        while (basicPattern != null) {
          basicPattern.printBasicPattern();
          joinHeapFile1.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());
          basicPattern = bpQuadHeapJoin.get_next();
        }
        bpQuadHeapJoin.close();
        //end of first join

        //start second join

        Heapfile joinHeapFile2 = new Heapfile("JOIN_HEAP_FILE2");
        if (fieldCount1 > 0) {
          bpFileScan = new BPFileScan("JOIN_HEAP_FILE1", fieldCount1);
          BPQuadIndexJoin bpQuadIndexJoin = new BPQuadIndexJoin(numBuf, fieldCount1, bpFileScan,
              query.getBpJoinNodePosition2(),
              query.getJoinOnSubjectOrObject2(), query.getRightSubjectFilter2(),
              query.getRightPredicateFilter2(), query.getRightObjectFilter2(),
              query.getRightConfidenceFilter2(), query.getLeftOutNodePositions2(),
              query.getOutputRightSubject2(), query.getOutputRightObject2());

          BasicPattern basicPattern1 = bpQuadIndexJoin.get_next();

          if (basicPattern1 != null) {
            fieldCount2 = basicPattern1.numberOfFields();
          }

          System.out.println("Printing Results after Second Join in 3.HeapfileIndexStrategy");
          while (basicPattern1 != null) {
            basicPattern1.printBasicPattern();
            joinHeapFile2.insertRecord(basicPattern1.getTupleFromBasicPattern().getTupleByteArray());
            basicPattern1 = bpQuadIndexJoin.get_next();
          }
          bpQuadIndexJoin.close();
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

            System.out.println("Printing results after sorting in 3.HeapfileIndexStrategy");

            while ((basicPattern = bpSort.get_next()) != null) {
              basicPattern.printBasicPattern();
            }

            bpSort.close();
          }
          joinHeapFile2.deleteFile();
          heapfile.deleteFile();
          System.out.println("After 3.HeapfileIndex Strategy : ");
          Telemetry.prinTelemetry();
          RDFSystemDefs.forceFlushBuffer();
        } catch (Exception e) {
          e.printStackTrace();
        }

      } else {
        System.out.println(
            "Correct Query Execution order of Parametres : RDFDBNAME QUERYFILE NUMBUF");
        return;
      }
      //End Strategy 3
    } catch (InvalidRelation e) {
      e.printStackTrace();
    } catch (FileScanException e) {
      e.printStackTrace();
    } catch (InvalidSlotNumberException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SpaceNotAvailableException e) {
      e.printStackTrace();
    } catch (FieldNumberOutOfBoundException e) {
      e.printStackTrace();
    } catch (HFDiskMgrException e) {
      e.printStackTrace();
    } catch (HFException e) {
      e.printStackTrace();
    } catch (HFBufMgrException e) {
      e.printStackTrace();
    } catch (InvalidTupleSizeException e) {
      e.printStackTrace();
    } catch (TupleUtilsException e) {
      e.printStackTrace();
    } catch (InvalidTypeException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }


  }

  @Override
  public void executeIndexIndexStrategy() {

    try {

      int fieldCount1 = 0;
      int fieldCount2 = 0;

      //Start strategy 4. Index join on both outer and inner join
      Heapfile heapfile = initHeapFile(false);

      if (heapfile.getRecCnt() > 0) {

        //Start First Join
        Heapfile joinHeapFile1 = new Heapfile("JOIN_HEAP_FILE1");
        BPFileScan bpFileScan = new BPFileScan("BP_HEAP", 3);

        BPQuadIndexJoin bpQuadIndexJoin = new BPQuadIndexJoin(numBuf, 3, bpFileScan,
            query.getBpJoinNodePosition1(),
            query.getJoinOnSubjectOrObject1(), query.getRightSubjectFilter1(),
            query.getRightPredicateFilter1(), query.getRightObjectFilter1(),
            query.getRightConfidenceFilter1(), query.getLeftOutNodePositions1(),
            query.getOutputRightSubject1(), query.getOutputRightObject1());

        BasicPattern basicPattern = null;
        basicPattern = bpQuadIndexJoin.get_next();

        if (basicPattern != null) {
          fieldCount1 = basicPattern.numberOfFields();
        }

        System.out.println("Printing results after first join in 4.IndexIndexStrategy");
        while (basicPattern != null) {
          basicPattern.printBasicPattern();
          joinHeapFile1.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());
          basicPattern = bpQuadIndexJoin.get_next();
        }
        bpQuadIndexJoin.close();
        //end of first join

        //start second join

        Heapfile joinHeapFile2 = new Heapfile("JOIN_HEAP_FILE2");
        if (fieldCount1 > 0) {
          bpFileScan = new BPFileScan("JOIN_HEAP_FILE1", fieldCount1);
          BPQuadIndexJoin bpQuadIndexJoin1 = new BPQuadIndexJoin(numBuf, fieldCount1, bpFileScan,
              query.getBpJoinNodePosition2(),
              query.getJoinOnSubjectOrObject2(), query.getRightSubjectFilter2(),
              query.getRightPredicateFilter2(), query.getRightObjectFilter2(),
              query.getRightConfidenceFilter2(), query.getLeftOutNodePositions2(),
              query.getOutputRightSubject2(), query.getOutputRightObject2());

          BasicPattern basicPattern1 = bpQuadIndexJoin1.get_next();

          if (basicPattern1 != null) {
            fieldCount2 = basicPattern1.numberOfFields();
          }

          System.out.println("Printing Results after Second Join in 4.IndexIndexStrategy");
          while (basicPattern1 != null) {
            basicPattern1.printBasicPattern();
            joinHeapFile2.insertRecord(basicPattern1.getTupleFromBasicPattern().getTupleByteArray());
            basicPattern1 = bpQuadIndexJoin1.get_next();
          }
          bpQuadIndexJoin1.close();
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

            System.out.println("Printing results after sorting in 4.IndexIndexStrategy : ");

            while ((basicPattern = bpSort.get_next()) != null) {
              basicPattern.printBasicPattern();
            }

            bpSort.close();
          }
          joinHeapFile2.deleteFile();
          heapfile.deleteFile();
          System.out.println("After 4.IndexIndexStrategy Strategy : ");
          Telemetry.prinTelemetry();
          RDFSystemDefs.forceFlushBuffer();
        } catch (Exception e) {
          e.printStackTrace();
        }

      } else {
        System.out.println(
            "Correct Query Execution order of Parametres : RDFDBNAME QUERYFILE NUMBUF");
        return;
      }

    } catch (InvalidRelation e) {
      e.printStackTrace();
    } catch (FileScanException e) {
      e.printStackTrace();
    } catch (InvalidSlotNumberException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SpaceNotAvailableException e) {
      e.printStackTrace();
    } catch (FieldNumberOutOfBoundException e) {
      e.printStackTrace();
    } catch (HFDiskMgrException e) {
      e.printStackTrace();
    } catch (HFException e) {
      e.printStackTrace();
    } catch (HFBufMgrException e) {
      e.printStackTrace();
    } catch (InvalidTupleSizeException e) {
      e.printStackTrace();
    } catch (TupleUtilsException e) {
      e.printStackTrace();
    } catch (InvalidTypeException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
