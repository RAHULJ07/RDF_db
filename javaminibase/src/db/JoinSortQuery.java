package db;

import global.BPOrder;
import java.util.Arrays;

public class JoinSortQuery {
  private String subjectFilter1;
  private String predicateFilter1;
  private String objectFilter1;
  private Float confidenceFilter1;

  private int bpJoinNodePosition1;
  private int joinOnSubjectOrObject1;
  private String rightSubjectFilter1;
  private String rightPredicateFilter1;
  private String rightObjectFilter1;
  private Float rightConfidenceFilter1;
  private int[] leftOutNodePositions1;
  private int outputRightSubject1;
  private int outputRightObject1;

  private int bpJoinNodePosition2;
  private int joinOnSubjectOrObject2;
  private String rightSubjectFilter2;
  private String rightPredicateFilter2;
  private String rightObjectFilter2;
  private Float rightConfidenceFilter2;
  private int[] leftOutNodePositions2;
  private int outputRightSubject2;
  private int outputRightObject2;

  private BPOrder sortOrder;
  private int sortNodeIdPos;

  public JoinSortQuery() {
  }

  public String getSubjectFilter1() {
    return subjectFilter1;
  }

  public void setSubjectFilter1(String subjectFilter1) {
    this.subjectFilter1 = subjectFilter1;
  }

  public String getPredicateFilter1() {
    return predicateFilter1;
  }

  public void setPredicateFilter1(String predicateFilter1) {
    this.predicateFilter1 = predicateFilter1;
  }

  public String getObjectFilter1() {
    return objectFilter1;
  }

  public void setObjectFilter1(String objectFilter1) {
    this.objectFilter1 = objectFilter1;
  }

  public Float getConfidenceFilter1() {
    return confidenceFilter1;
  }

  public void setConfidenceFilter1(Float confidenceFilter1) {
    this.confidenceFilter1 = confidenceFilter1;
  }

  public int getBpJoinNodePosition1() {
    return bpJoinNodePosition1;
  }

  public void setBpJoinNodePosition1(int bpJoinNodePosition1) {
    this.bpJoinNodePosition1 = bpJoinNodePosition1;
  }

  public int getJoinOnSubjectOrObject1() {
    return joinOnSubjectOrObject1;
  }

  public void setJoinOnSubjectOrObject1(int joinOnSubjectOrObject1) {
    this.joinOnSubjectOrObject1 = joinOnSubjectOrObject1;
  }

  public String getRightSubjectFilter1() {
    return rightSubjectFilter1;
  }

  public void setRightSubjectFilter1(String rightSubjectFilter1) {
    this.rightSubjectFilter1 = rightSubjectFilter1;
  }

  public String getRightPredicateFilter1() {
    return rightPredicateFilter1;
  }

  public void setRightPredicateFilter1(String rightPredicateFilter1) {
    this.rightPredicateFilter1 = rightPredicateFilter1;
  }

  public String getRightObjectFilter1() {
    return rightObjectFilter1;
  }

  public void setRightObjectFilter1(String rightObjectFilter1) {
    this.rightObjectFilter1 = rightObjectFilter1;
  }

  public Float getRightConfidenceFilter1() {
    return rightConfidenceFilter1;
  }

  public void setRightConfidenceFilter1(Float rightConfidenceFilter1) {
    this.rightConfidenceFilter1 = rightConfidenceFilter1;
  }

  public int[] getLeftOutNodePositions1() {
    return leftOutNodePositions1;
  }

  public void setLeftOutNodePositions1(int[] leftOutNodePositions1) {
    this.leftOutNodePositions1 = leftOutNodePositions1;
  }

  public int getOutputRightSubject1() {
    return outputRightSubject1;
  }

  public void setOutputRightSubject1(int outputRightSubject1) {
    this.outputRightSubject1 = outputRightSubject1;
  }

  public int getOutputRightObject1() {
    return outputRightObject1;
  }

  public void setOutputRightObject1(int outputRightObject1) {
    this.outputRightObject1 = outputRightObject1;
  }

  public int getBpJoinNodePosition2() {
    return bpJoinNodePosition2;
  }

  public void setBpJoinNodePosition2(int bpJoinNodePosition2) {
    this.bpJoinNodePosition2 = bpJoinNodePosition2;
  }

  public int getJoinOnSubjectOrObject2() {
    return joinOnSubjectOrObject2;
  }

  public void setJoinOnSubjectOrObject2(int joinOnSubjectOrObject2) {
    this.joinOnSubjectOrObject2 = joinOnSubjectOrObject2;
  }

  public String getRightSubjectFilter2() {
    return rightSubjectFilter2;
  }

  public void setRightSubjectFilter2(String rightSubjectFilter2) {
    this.rightSubjectFilter2 = rightSubjectFilter2;
  }

  public String getRightPredicateFilter2() {
    return rightPredicateFilter2;
  }

  public void setRightPredicateFilter2(String rightPredicateFilter2) {
    this.rightPredicateFilter2 = rightPredicateFilter2;
  }

  public String getRightObjectFilter2() {
    return rightObjectFilter2;
  }

  public void setRightObjectFilter2(String rightObjectFilter2) {
    this.rightObjectFilter2 = rightObjectFilter2;
  }

  public Float getRightConfidenceFilter2() {
    return rightConfidenceFilter2;
  }

  public void setRightConfidenceFilter2(Float rightConfidenceFilter2) {
    this.rightConfidenceFilter2 = rightConfidenceFilter2;
  }

  public int[] getLeftOutNodePositions2() {
    return leftOutNodePositions2;
  }

  public void setLeftOutNodePositions2(int[] leftOutNodePositions2) {
    this.leftOutNodePositions2 = leftOutNodePositions2;
  }

  public int getOutputRightSubject2() {
    return outputRightSubject2;
  }

  public void setOutputRightSubject2(int outputRightSubject2) {
    this.outputRightSubject2 = outputRightSubject2;
  }

  public int getOutputRightObject2() {
    return outputRightObject2;
  }

  public void setOutputRightObject2(int outputRightObject2) {
    this.outputRightObject2 = outputRightObject2;
  }

  public BPOrder getSortOrder() {
    return sortOrder;
  }

  public void setSortOrder(BPOrder sortOrder) {
    this.sortOrder = sortOrder;
  }

  public int getSortNodeIdPos() {
    return sortNodeIdPos;
  }

  public void setSortNodeIdPos(int sortNodeIdPos) {
    this.sortNodeIdPos = sortNodeIdPos;
  }

  @Override
  public String toString() {
    return "JoinSortQuery{" +
        "subjectFilter1='" + subjectFilter1 + '\'' +
        ", predicateFilter1='" + predicateFilter1 + '\'' +
        ", objectFilter1='" + objectFilter1 + '\'' +
        ", confidenceFilter1=" + confidenceFilter1 +
        ", bpJoinNodePosition1=" + bpJoinNodePosition1 +
        ", joinOnSubjectOrObject1=" + joinOnSubjectOrObject1 +
        ", rightSubjectFilter1='" + rightSubjectFilter1 + '\'' +
        ", rightPredicateFilter1='" + rightPredicateFilter1 + '\'' +
        ", rightObjectFilter1='" + rightObjectFilter1 + '\'' +
        ", rightConfidenceFilter1=" + rightConfidenceFilter1 +
        ", leftOutNodePositions1=" + Arrays.toString(leftOutNodePositions1) +
        ", outputRightSubject1=" + outputRightSubject1 +
        ", outputRightObject1=" + outputRightObject1 +
        ", bpJoinNodePosition2=" + bpJoinNodePosition2 +
        ", joinOnSubjectOrObject2=" + joinOnSubjectOrObject2 +
        ", rightSubjectFilter2='" + rightSubjectFilter2 + '\'' +
        ", rightPredicateFilter2='" + rightPredicateFilter2 + '\'' +
        ", rightObjectFilter2='" + rightObjectFilter2 + '\'' +
        ", rightConfidenceFilter2=" + rightConfidenceFilter2 +
        ", leftOutNodePositions2=" + Arrays.toString(leftOutNodePositions2) +
        ", outputRightSubject2=" + outputRightSubject2 +
        ", outputRightObject2=" + outputRightObject2 +
        ", sortOrder=" + sortOrder +
        ", sortNodeIdPos=" + sortNodeIdPos +
        '}';
  }
}
