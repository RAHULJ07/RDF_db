package db;

import global.BPOrder;

public class JoinSortQueryFileParser {
  private String queryString;

  public JoinSortQueryFileParser(String queryString) {
    this.queryString = queryString;
  }

  public JoinSortQuery parseQuery() {

    queryString = queryString.replaceAll("\\s", "");

    String subjectFilter1;
    String predicateFilter1;
    String objectFilter1;
    Float confidenceFilter1;

    int bpJoinNodePosition1;
    int joinOnSubjectOrObject1;
    String rightSubjectFilter1;
    String rightPredicateFilter1;
    String rightObjectFilter1;
    Float rightConfidenceFilter1;
    int[] leftOutNodePositions1;
    int outputRightSubject1;
    int outputRightObject1;

    int bpJoinNodePosition2;
    int joinOnSubjectOrObject2;
    String rightSubjectFilter2;
    String rightPredicateFilter2;
    String rightObjectFilter2;
    Float rightConfidenceFilter2;
    int[] leftOutNodePositions2;
    int outputRightSubject2;
    int outputRightObject2;

    BPOrder sortOrder;
    int sortNodeIDPos;
    int numberOfPages;

    int start = queryString.indexOf("(") + 1;
    int end = queryString.lastIndexOf(")");

    String sortParameters = queryString.substring(start, end);

    String sortRightParameters = sortParameters.substring(sortParameters.lastIndexOf(")") + 1, sortParameters.length());
    String[] tokens = sortRightParameters.split(",");
    if(Integer.parseInt(tokens[0]) == 0) {
      sortOrder = new BPOrder(0); //Ascending
    } else if(Integer.parseInt(tokens[0]) == 1) {
      sortOrder = new BPOrder(1); //Descending
    } else if(Integer.parseInt(tokens[0]) == 2) {
      sortOrder = new BPOrder(3); //Random
    } else {
      sortOrder = null;
    }

    sortNodeIDPos = Integer.parseInt(tokens[1]);

    numberOfPages = Integer.parseInt(tokens[2]);

    start = sortParameters.indexOf("(") + 1;
    end = sortParameters.lastIndexOf(")");

    String outerJoinParameters = sortParameters.substring(start, end);

    String outerJoinRightParameters = outerJoinParameters.substring(outerJoinParameters.indexOf(")") + 2, outerJoinParameters.length());

    String outerJoinRightParameters1 = outerJoinRightParameters.substring(0, outerJoinRightParameters.indexOf("{") - 1);
    tokens = outerJoinRightParameters1.split(",");

    bpJoinNodePosition2 = Integer.parseInt(tokens[0]);
    joinOnSubjectOrObject2 = Integer.parseInt(tokens[1]);

    if(tokens[2].equals("*")) {
      rightSubjectFilter2 = null;
    } else {
      rightSubjectFilter2 = tokens[2].replaceFirst("^:", "").replaceAll("\"", "");
    }

    if(tokens[3].equals("*")) {
      rightPredicateFilter2 = null;
    } else {
      rightPredicateFilter2 = tokens[3].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[4].equals("*")) {
      rightObjectFilter2 = null;
    } else {
      rightObjectFilter2 = tokens[4].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[5].equals("*")) {
      rightConfidenceFilter2 = null;
    } else {
      rightConfidenceFilter2 = Float.parseFloat(tokens[5]);
    }

    start = outerJoinRightParameters.indexOf("{") + 1;
    end = outerJoinRightParameters.lastIndexOf("}");

    String outerJoinLeftOutNodePositions = outerJoinRightParameters.substring(start, end);
    tokens = outerJoinLeftOutNodePositions.split(",");
    
    leftOutNodePositions2 = new int[tokens.length];
    for(int i = 0; i < tokens.length; i++) {
      leftOutNodePositions2[i] = Integer.parseInt(tokens[i]);
    }

    String outerJoinRightParameters2 = outerJoinRightParameters.substring(outerJoinRightParameters.indexOf("}") + 2, outerJoinRightParameters.length());
    tokens = outerJoinRightParameters2.split(",");

    outputRightSubject2 = Integer.parseInt(tokens[0]);
    outputRightObject2 = Integer.parseInt(tokens[1]);

    start = outerJoinParameters.indexOf("(") + 1;
    end = outerJoinParameters.lastIndexOf(")");
    String innerJoinParameters = outerJoinParameters.substring(start, end);

    String innerJoinRightParameters = innerJoinParameters.substring(innerJoinParameters.indexOf("]") + 2, innerJoinParameters.length());

    String innerJoinRightParameters1 = innerJoinRightParameters.substring(0, innerJoinRightParameters.indexOf("{") - 1);
    tokens = innerJoinRightParameters1.split(",");

    bpJoinNodePosition1 = Integer.parseInt(tokens[0]);
    joinOnSubjectOrObject1 = Integer.parseInt(tokens[1]);

    if(tokens[2].equals("*")) {
      rightSubjectFilter1 = null;
    } else {
      rightSubjectFilter1 = tokens[2].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[3].equals("*")) {
      rightPredicateFilter1 = null;
    } else {
      rightPredicateFilter1 = tokens[3].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[4].equals("*")) {
      rightObjectFilter1 = null;
    } else {
      rightObjectFilter1 = tokens[4].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[5].equals("*")) {
      rightConfidenceFilter1 = null;
    } else {
      rightConfidenceFilter1 = Float.parseFloat(tokens[5]);
    }

    start = innerJoinRightParameters.indexOf("{") + 1;
    end = innerJoinRightParameters.lastIndexOf("}");

    String innerJoinLeftOutNodePositions = innerJoinRightParameters.substring(start, end);

    tokens = innerJoinLeftOutNodePositions.split(",");
    leftOutNodePositions1 = new int[tokens.length];
    for(int i = 0; i < tokens.length; i++) {
        leftOutNodePositions1[i] = Integer.parseInt(tokens[i]);
    }

    String innerJoinRightParameters2 = innerJoinRightParameters.substring(innerJoinRightParameters.indexOf("}") + 2, innerJoinRightParameters.length());
    tokens = innerJoinRightParameters2.split(",");
    
    outputRightSubject1 = Integer.parseInt(tokens[0]);
    outputRightObject1 = Integer.parseInt(tokens[1]);

    start = queryString.indexOf("[") + 1;
    end = queryString.lastIndexOf("]");

    String innerJoinLeftParameters = queryString.substring(start, end);
    
    tokens = innerJoinLeftParameters.split(",");

    if(tokens[0].equals("*")) {
      subjectFilter1 = null;
    } else {
      subjectFilter1 = tokens[0].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[1].equals("*")) {
      predicateFilter1 = null;
    } else {
      predicateFilter1 = tokens[1].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[2].equals("*")) {
      objectFilter1 = null;
    } else {
      objectFilter1 = tokens[2].replaceAll("\"", "").replaceFirst("^:", "");
    }

    if(tokens[3].equals("*")) {
      confidenceFilter1 = null;
    } else {
      confidenceFilter1 = Float.parseFloat(tokens[3]);
    }

    JoinSortQuery query = new JoinSortQuery();

    query.setSubjectFilter1(subjectFilter1);
    query.setPredicateFilter1(predicateFilter1);
    query.setObjectFilter1(objectFilter1);
    query.setConfidenceFilter1(confidenceFilter1);

    query.setBpJoinNodePosition1(bpJoinNodePosition1);
    query.setJoinOnSubjectOrObject1(joinOnSubjectOrObject1);
    query.setRightSubjectFilter1(rightSubjectFilter1);
    query.setRightPredicateFilter1(rightPredicateFilter1);
    query.setRightObjectFilter1(rightObjectFilter1);
    query.setRightConfidenceFilter1(rightConfidenceFilter1);
    query.setLeftOutNodePositions1(leftOutNodePositions1);
    query.setOutputRightSubject1(outputRightSubject1);
    query.setOutputRightObject1(outputRightObject1);

    query.setBpJoinNodePosition2(bpJoinNodePosition2);
    query.setJoinOnSubjectOrObject2(joinOnSubjectOrObject2);
    query.setRightSubjectFilter2(rightSubjectFilter2);
    query.setRightPredicateFilter2(rightPredicateFilter2);
    query.setRightObjectFilter2(rightObjectFilter2);
    query.setRightConfidenceFilter2(rightConfidenceFilter2);
    query.setLeftOutNodePositions2(leftOutNodePositions2);
    query.setOutputRightSubject2(outputRightSubject2);
    query.setOutputRightObject2(outputRightObject2);

    query.setSortOrder(sortOrder);
    query.setSortNodeIDPos(sortNodeIDPos);
    query.setNumberOfPages(numberOfPages);

    return query;
  }
}
