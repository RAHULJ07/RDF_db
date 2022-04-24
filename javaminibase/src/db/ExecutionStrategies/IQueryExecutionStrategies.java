package db.ExecutionStrategies;

import db.InnerJoinOption;

public interface IQueryExecutionStrategies {

  void execute(InnerJoinOption queryJoin1, InnerJoinOption queryJoin2) throws Exception;
}
