package basicpattern;

import diskmgr.rdf.IStream;
import global.EID;
import heap.FieldNumberOutOfBoundException;
import heap.Quadruple;
import iterator.QuadrupleUtils;
import java.io.IOException;
import java.util.ArrayList;
import bpiterator.BPIterator;

public abstract class BaseBPQuadJoin implements IBPQuadJoin{

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
  private boolean isLeftEOF;
  private BasicPattern outerTuple;
  private Quadruple innerTuple;
  IStream stream;

  /**
   * Constructor for Base Bp Quad Join
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
  public BaseBPQuadJoin(int amt_of_mem,
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
    this.RightSubjectFilter = RightSubjectFilter;
    this.RightObjectFilter = RightObjectFilter;
    this.RightPredicateFilter = RightPredicateFilter;
    this.RightConfidenceFilter = RightConfidenceFilter;
    this.LeftOutNodePosition = LeftOutNodePosition;
    this.OutputRightSubject = OutputRightSubject;
    this.OutputRightObject = OutputRightObject;
    outerTuple = null;
    innerTuple = null;
    stream = null;

    try {
      stream = getStream();
      outerTuple = left_iter.get_next();
    } catch (Exception e) {
      System.err.println("Error while opening a stream");
      e.printStackTrace();
    }
  }

  /**
   * get the next matching basic pattern
   * @return BasicPattern
   * @throws Exception
   */
  @Override
  public BasicPattern get_next() throws Exception {
    if(isLeftEOF)
    {
      return null;
    }

    if (outerTuple  == null) {
      //EOF reached for outer file
      isLeftEOF = true;
      if (stream != null) {
        stream.closeStream();
        stream = null;
      }
      return null;
    }


    Quadruple quad = stream.getNext();


    while (quad != null) {
      BasicPattern matchedPattern = joinProject(quad);
      if (matchedPattern == null) {
        quad = stream.getNext();
      } else {
        return matchedPattern;
      }
    }

    // Exhausted inner loop, restart the inner loop and increase the outer iterator
    stream.closeStream();

    try {
      stream = getStream();
    } catch (Exception e) {
      System.err.println("Error while opening a stream");
      e.printStackTrace();
    }

    if ((outerTuple = left_iter.get_next()) == null) {
      //EOF reached for outer file
      isLeftEOF = true;
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

  /**
   * performs join and projection for given quadruple
   * @param quad
   * @return Basic Pattern
   * @throws Exception
   */
  private BasicPattern joinProject(Quadruple quad) throws Exception {
    EID eidOuter = outerTuple.getEIDFld(BPJoinNodePosition + 1);
    String eidOuterLabel = QuadrupleUtils.entityHeapFile.getLabel(eidOuter.returnLID()).getLabel();
    EID eidInner;

    if (JoinOnSubjectorObject == 0)
      eidInner = (EID) quad.getSubjectID();
    else
      eidInner = (EID) quad.getObjectID();

    if (eidOuter.equals(eidInner)) {
//      quad.print();
      BasicPattern bp = new BasicPattern();

      boolean isJoinNodeProjected = false;
      ArrayList<EID> nodeIDs = new ArrayList<>();

      for (int nodeIdx = 0; nodeIdx < LeftOutNodePosition.length; nodeIdx++) {
        nodeIDs.add(outerTuple.getEIDFld(LeftOutNodePosition[nodeIdx]+1));
        if (LeftOutNodePosition[nodeIdx] == BPJoinNodePosition) {
          isJoinNodeProjected = true; //join node projected from leftOutNodePosition
        }
      }

      //Two case for inner projection given OutputRightObject
      //1) when left out nodes doesnot contain join node
      //2) when join is on subject
      if (OutputRightObject == 1 && (!isJoinNodeProjected || JoinOnSubjectorObject == 0)) {
        nodeIDs.add((EID) quad.getObjectID());
      }

      //Two case for inner projection given OutputRightSubject
      //1) when left out nodes doesnot contain join node
      //2) when join is on object
      if (OutputRightSubject == 1 && (!isJoinNodeProjected || JoinOnSubjectorObject == 1)) {
        nodeIDs.add((EID) quad.getSubjectID());
      }

      double minConfidence = getMinConfidence(quad);

      return getBPfromNodearray(bp, nodeIDs, minConfidence);
    }
    return null; // eid for join was not a match.
  }

  /**
   * get basic pattern from node array
   * @param bp
   * @param nodeIDs
   * @param minConfidence
   * @return
   */
  private BasicPattern getBPfromNodearray(BasicPattern bp, ArrayList<EID> nodeIDs,
      double minConfidence) throws Exception {
    if (nodeIDs.size() != 0) {
      int nodeidx;

      bp.setHeader((short) (nodeIDs.size() + 1));
      for (nodeidx = 0; nodeidx < nodeIDs.size(); nodeidx++) {
        bp.setEIDField(nodeidx + 1, nodeIDs.get(nodeidx));
      }
      bp.setDoubleFld(nodeidx + 1, minConfidence);
      return bp;
    }
    return null;
  }

  /**
   * get minimum confidence
   * @param quad
   * @return confidence
   * @throws FieldNumberOutOfBoundException
   * @throws IOException
   */
  private double getMinConfidence(Quadruple quad)
      throws FieldNumberOutOfBoundException, IOException {
    double minConfidence = 0.0;
    if (quad.getConfidence() <= outerTuple.getDoubleFld(outerTuple.numberOfFields()))
      minConfidence = quad.getConfidence();
    else
      minConfidence = outerTuple.getDoubleFld(outerTuple.numberOfFields());
    return minConfidence;
  }

  /**
   * close join operation
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    try{
      if(stream != null){
        stream.closeStream();
      }
      left_iter.close();
    }catch(Exception e){
      System.err.println("Error while closing join");
      e.printStackTrace();
    }
  }
}
