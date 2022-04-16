package db;

public class DBJoinSortQueryParser implements IParser {
  public IQuery parse(String query) throws IllegalArgumentException {
    /**
     * RDFDBNAME QUERYFILE NUMBUF INDEXOPTION
     */

    String[] tokens = query.split(" ");

    if(tokens.length != 4) {
      throw new IllegalArgumentException("Number of arguments are not equal to 4.");
    }

    return new DBJoinSortQuery(
      tokens[0],
      tokens[1],
      tokens[2],
      tokens[3]
    );
  }
}
