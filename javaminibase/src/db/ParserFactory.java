package db;

/**
 * Factory design pattern for Parser
 * <p>
 * Can provide the concrete objects
 * for :-
 * 1. InsertQueryParser
 * 2. SelectQueryParser
 * 3. DBJoinSortQueryParser
 */
public class ParserFactory {
  public static IParser getParser(QueryType queryType) {
    if (queryType == null) {
      return null;
    }

    if (queryType == QueryType.INSERT) {
      return new InsertQueryParser();
    } else if (queryType == QueryType.SELECT) {
      return new SelectQueryParser();
    } else if(queryType == QueryType.JOIN_SORT) {
      return new DBJoinSortQueryParser();
    }
    return null;
  }
}
