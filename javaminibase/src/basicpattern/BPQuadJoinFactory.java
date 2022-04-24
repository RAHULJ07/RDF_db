package basicpattern;

import bpiterator.BPIterator;
import db.InnerJoinOption;

public class BPQuadJoinFactory {


  public static IBPQuadJoin createBPQuadJoin(InnerJoinOption innerJoinOption
      ,int amt_of_mem,
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
      int OutputRightObject)
      throws Exception {

    IBPQuadJoin bpQuadJoin = null;
    switch (innerJoinOption) {
      case IndexScan: {
        bpQuadJoin = new BPQuadIndexJoin(amt_of_mem, num_left_nodes, left_iter, BPJoinNodePosition
            , JoinOnSubjectorObject, RightSubjectFilter, RightPredicateFilter, RightObjectFilter
            , RightConfidenceFilter, LeftOutNodePosition, OutputRightSubject, OutputRightObject);
        break;
      }

      case HeapScan:
        bpQuadJoin = new BPQuadHeapJoin(amt_of_mem, num_left_nodes, left_iter, BPJoinNodePosition
            , JoinOnSubjectorObject, RightSubjectFilter, RightPredicateFilter, RightObjectFilter
            , RightConfidenceFilter, LeftOutNodePosition, OutputRightSubject, OutputRightObject);
        break;
    }
    return bpQuadJoin;
  }
}
