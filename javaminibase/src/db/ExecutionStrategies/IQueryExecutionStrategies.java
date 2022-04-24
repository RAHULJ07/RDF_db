package db.ExecutionStrategies;

public interface IQueryExecutionStrategies {

  public void executeHeapfileHeapfileStrategy() throws Exception;
  public void executeHeapfileIndexStrategy();
  public void executeIndexIndexStrategy();
  public void executeIndexHeapfileStrategy();

}
