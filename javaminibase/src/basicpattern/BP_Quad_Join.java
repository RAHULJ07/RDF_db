package basicpattern;

import diskmgr.rdf.IStream;
import diskmgr.rdf.RdfDB;
import global.EID;
import global.QuadOrder;
import global.SystemDefs;
import heap.Quadruple;
import heap.quadrupleheap.QuadrupleHeapFile;
import java.util.ArrayList;
import java.util.stream.Stream;

public class BP_Quad_Join {

  int amt_of_mem;
  int num_left_nodes;
  BPIterator left_iter;
  int BPJoinNodePosition;
  int JoinOnSubjectorObject;
  String RightSubjectFilter;
  String RightPredicateFilter;
  String RightObjectFilter;
  Float RightConfidenceFilter;
  int[] LeftOutNodePosition;
  int OutputRightSubject;
  int OutputRightObject;
  private boolean done;         // Is the join
  private boolean get_from_outer;                 // if TRUE, a tuple is got from outer
  //private   TScan      inner; //XXX
  private Stream inner;
  private BasicPattern outer_tuple;
  private Quadruple inner_tuple;
  QuadrupleHeapFile Quad_HF;
  IStream stream;

  public BP_Quad_Join(int amt_of_mem,
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
    this.amt_of_mem = amt_of_mem;
    this.num_left_nodes = num_left_nodes;
    this.left_iter = left_iter;
    this.BPJoinNodePosition = BPJoinNodePosition;
    this.JoinOnSubjectorObject = JoinOnSubjectorObject;
    this.RightSubjectFilter = new String(RightSubjectFilter);
    this.RightObjectFilter = new String(RightObjectFilter);
    this.RightPredicateFilter = new String(RightPredicateFilter);
    this.RightConfidenceFilter = RightConfidenceFilter;
    this.LeftOutNodePosition = LeftOutNodePosition;
    this.OutputRightSubject = OutputRightSubject;
    this.OutputRightObject = OutputRightObject;
    get_from_outer = true;
    inner = null;
    outer_tuple = null;
    inner_tuple = null;
    done = false;
    stream = null;

    try {
      stream = ((RdfDB) SystemDefs.JavabaseDB).openStream(
          new QuadOrder(QuadOrder.SubjectPredicateObjectConfidence),
          this.amt_of_mem,
          this.RightSubjectFilter,
          this.RightPredicateFilter,
          this.RightObjectFilter,
          this.RightConfidenceFilter
      );
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public BasicPattern get_next() throws Exception {

    Quadruple quad = stream.getNext();
    while (quad != null) {
        BasicPattern matchedPattern = algo(quad);
        if(matchedPattern == null) {
          quad = stream.getNext();
        }
        else{
          return matchedPattern;
        }
    }

    stream.closeStream();
    try {
      stream = ((RdfDB) SystemDefs.JavabaseDB).openStream(
          new QuadOrder(QuadOrder.SubjectPredicateObjectConfidence),
          this.amt_of_mem,
          this.RightSubjectFilter,
          this.RightPredicateFilter,
          this.RightObjectFilter,
          this.RightConfidenceFilter
      );
    } catch (Exception e) {
      e.printStackTrace();
    }

    if ((outer_tuple = left_iter.get_next()) == null) {
      done = true;
      if (stream != null) {
        stream.closeStream();
        stream = null;
      }
    return null;
  }
  else
  {
    return get_next();
  }
}


  private BasicPattern algo(Quadruple quad) throws Exception {
    ArrayList<EID> arrEID = new ArrayList<>();

    EID eidOuter = outer_tuple.getEIDField(BPJoinNodePosition);
    EID eidInner;

    if (JoinOnSubjectorObject == 0)
      eidInner = (EID) inner_tuple.getSubjectID();
    else
      eidInner = (EID) inner_tuple.getObjectID();

    double minConfidence = 0.0;
    if (quad.getConfidence() <= outer_tuple.getDoubleFld(outer_tuple.numberOfFlds()))
      minConfidence = quad.getConfidence();
    else
      minConfidence = outer_tuple.getDoubleFld(outer_tuple.numberOfFlds());

    if (eidOuter.equals(eidInner)) {
      BasicPattern bp = new BasicPattern();

      boolean isJoinNodeProjected = false;

      for (int nodeIdx = 0; nodeIdx < LeftOutNodePosition.length; nodeIdx++) {
        arrEID.add(outer_tuple.getEIDField(LeftOutNodePosition[nodeIdx]));
        if (LeftOutNodePosition[nodeIdx] == BPJoinNodePosition) {
          isJoinNodeProjected = true;
        }
      }

      if (OutputRightSubject == 1 && (!isJoinNodeProjected || JoinOnSubjectorObject == 1)) {
        arrEID.add((EID) inner_tuple.getObjectID());
      }

      if (OutputRightSubject == 1 && (!isJoinNodeProjected || JoinOnSubjectorObject == 0)) {
        arrEID.add((EID) inner_tuple.getSubjectID());
      }

      if (arrEID.size() != 0) {
        int k;
        bp.setHdr((short) (arrEID.size() + 1));
        for (k = 0; k < arrEID.size(); k++) {
          bp.setEIDField(k + 1, arrEID.get(k));
        }
        bp.setDoubleFld(k + 1, minConfidence);
        return bp;
      }
    }
    return null;
  }


}
