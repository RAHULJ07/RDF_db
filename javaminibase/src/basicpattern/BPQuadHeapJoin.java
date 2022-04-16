package basicpattern;

import diskmgr.rdf.IStream;
import diskmgr.rdf.RdfDB;
import global.QuadOrder;
import global.SystemDefs;
import bpiterator.BPIterator;

public class BPQuadHeapJoin extends BaseBPQuadJoin {

  /**
   * Constructor for BP Quad Heap Join
   * @param amt_of_mem
   * @param num_left_nodes
   * @param left_iter
   * @param BPJoinNodePosition
   * @param JoinOnSubjectorObject
   * @param RightSubjectFilter
   * @param RightPredicateFilter
   * @param RightObjectFilter
   * @param RightConfidenceFilter
   * @param LeftOutNodePosition
   * @param OutputRightSubject
   * @param OutputRightObject
   */
  public BPQuadHeapJoin(int amt_of_mem,
      int num_left_nodes,
      BPIterator left_iter,
      int BPJoinNodePosition,
      int JoinOnSubjectorObject,
      String RightSubjectFilter,
      String RightPredicateFilter,
      String RightObjectFilter,
      Float RightConfidenceFilter,
      int[] LeftOutNodePosition,
      int OutputRightSubject,
      int OutputRightObject) {
    super(amt_of_mem, num_left_nodes, left_iter, BPJoinNodePosition, JoinOnSubjectorObject, RightSubjectFilter,
        RightPredicateFilter, RightObjectFilter, RightConfidenceFilter, LeftOutNodePosition,
        OutputRightSubject, OutputRightObject);
  }

  /**
   * get Stream for heap file scan
   * @return Stream
   * @throws Exception
   */
  public IStream getStream() throws Exception {
    return ((RdfDB) SystemDefs.JavabaseDB).openTStream(
        new QuadOrder(QuadOrder.NoOrder),
        this.amt_of_mem,
        this.RightSubjectFilter,
        this.RightPredicateFilter,
        this.RightObjectFilter,
        this.RightConfidenceFilter
    );
  }
}
