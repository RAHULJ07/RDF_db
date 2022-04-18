package db;

import basicpattern.BPQuadHeapJoin;
import basicpattern.BasicPattern;
import bpiterator.BPFileScan;
import bpiterator.BPSort;
import btree.lablebtree.BTFileScan;
import diskmgr.rdf.RdfDB;
import diskmgr.rdf.TStream;
import global.BPOrder;
import global.QuadOrder;
import global.SystemDefs;
import heap.Heapfile;
import java.util.stream.Stream;

public class DBJoinSortQuery extends BaseQuery implements IQuery {

  private String queryFile;
  private int numBuf;
  private IndexOption indexOption;

  public DBJoinSortQuery(String dbName,
      String queryFile,
      String numBuf,
      String indexOption) {
    super(dbName);
    this.queryFile = queryFile;
    this.numBuf = Integer.parseInt(numBuf);
    this.indexOption = IndexOption.valueOf(indexOption);
  }

  public String getQueryFile() {
    return queryFile;
  }

  public void setQueryFile(String queryFile) {
    this.queryFile = queryFile;
  }

  public int getNumBuf() {
    return numBuf;
  }

  public void setNumBuf(String numBuf) {
    this.numBuf = Integer.valueOf(numBuf);
  }

  public IndexOption getIndexOption() {
    return indexOption;
  }

  public void setIndexOption(String indexOption) {
    this.indexOption = IndexOption.valueOf(indexOption);
  }

  @Override
  public void execute() throws Exception {
    JoinSortQueryFileReader fileReader = new JoinSortQueryFileReader(queryFile);
    JoinSortQuery query = fileReader.getQuery();

    //Query Execute
    int fieldCount1 = 0;
    int fieldCount2 = 0;

    QuadOrder quadOrder = new QuadOrder(7);
    TStream stream = ((RdfDB) SystemDefs.JavabaseDB).openTStream(quadOrder, numBuf,
        query.getSubjectFilter1(), query.getPredicateFilter1(), query.getObjectFilter1(),
        query.getConfidenceFilter1());

    Heapfile heapfile = new Heapfile("BP_HEAPFILE");
    BasicPattern basicPattern = null;

    System.out.println("Printing  Basic Patterns from BP HeapFile");

    while ((basicPattern = stream.getNextBasicPatternFromQuadruple()) != null) {
      basicPattern.printBasicPattern();
      heapfile.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());

    }

    if (stream != null) {
      stream.closeStream();
    }

    if (heapfile.getRecCnt() > 0) {

      //Start First Join
      Heapfile joinHeapFile1 = new Heapfile("JOIN_HEAP_FILE1");
      BPFileScan bpFileScan = new BPFileScan("BP_HEAPFILE", 3);

      BPQuadHeapJoin bpQuadHeapJoin = new BPQuadHeapJoin(numBuf, 3, bpFileScan,
          query.getBpJoinNodePosition1(),
          query.getJoinOnSubjectOrObject1(), query.getRightSubjectFilter1(),
          query.getRightPredicateFilter1(), query.getRightObjectFilter1(),
          query.getRightConfidenceFilter1(), query.getLeftOutNodePositions1(),
          query.getOutputRightSubject1(), query.getOutputRightObject1());

      basicPattern = bpQuadHeapJoin.get_next();
      fieldCount1 = basicPattern.numberOfFields();

      System.out.println("Printing results after first join : ");
      while (basicPattern != null) {
        basicPattern.printBasicPattern();
        joinHeapFile1.insertRecord(basicPattern.getTupleFromBasicPattern().getTupleByteArray());
        basicPattern = bpQuadHeapJoin.get_next();
      }
      bpQuadHeapJoin.close();
      heapfile.deleteFile();

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
        fieldCount2 = basicPattern1.numberOfFields();

        System.out.println("Printing Results after Second Join");
        while (basicPattern1 != null) {
          basicPattern1.printBasicPattern();
          joinHeapFile2.insertRecord(basicPattern1.getTupleFromBasicPattern().getTupleByteArray());
          basicPattern1 = bpQuadHeapJoin.get_next();
        }
        bpQuadHeapJoin.close();
      }

      joinHeapFile1.deleteFile();

      if (joinHeapFile2.getRecCnt() > 0) {
        BPFileScan bpFileScan2 = null;
        try {
          bpFileScan2 = new BPFileScan("JOIN_HEAP_FILE2", fieldCount2);
        } catch (Exception e) {
          e.printStackTrace();
        }

        BPSort bpSort = null;
        BPOrder bpOrder = query.getSortOrder();
        try {
          bpSort = new BPSort(bpFileScan2, bpOrder, query.getSortNodeIDPos(),
              query.getNumberOfPages());
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("Printing results after sorting : ");

        try {
          while ((basicPattern = bpSort.get_next()) != null) {
            basicPattern.printBasicPattern();
            ;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        bpSort.close();
      }
      joinHeapFile2.deleteFile();
    } else {
      System.out.println(
          "Correct Query Execution order of Parametres : RDFDBNAME QUERYFILE NUMBUF");
      return;
    }
    //

  }
}
