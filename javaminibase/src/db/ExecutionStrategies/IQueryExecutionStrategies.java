package db.ExecutionStrategies;

import db.JoinOption;

public interface IQueryExecutionStrategies {

  void execute(JoinOption queryJoin1, JoinOption queryJoin2) throws Exception;
}
