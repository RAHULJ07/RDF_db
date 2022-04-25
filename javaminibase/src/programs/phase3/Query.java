package programs.phase3;

import db.DBJoinSortQuery;
import db.QueryFactory;
import db.QueryType;
import db.RDFDatabase;

/**
 * This is the top most class which executes the query as
 * defined in phase 3 specification. It takes three arguments.
 * The RDFDB name, the path to the query file and the maximum
 * number of buffer pages which can be used to run the query.
 */
public class Query {
  public static void main(String[] args) throws Exception {
//    query RDFDBNAME QUERYFILE NUMBUF INDEXOPTION

    if(args.length != 4) {
      System.out.println("Please provide all the required arguments.");
      System.out.println("1st argument is the RDFDB name");
      System.out.println("2nd argument is the path to the query file.");
      System.out.println("3rd argument is the maximum number of buffer pages to run the query.");
      System.out.println("4th argument is the index option which was used to create the DB");
      return;
    }
    DBJoinSortQuery query = (DBJoinSortQuery) QueryFactory.getQuery(QueryType.JOIN_SORT, String.join(" ", args));
    RDFDatabase db = new RDFDatabase(query.getDbName(), query.getIndexOption());
    db.joinSort(query, 1);
    db.close();

    db = new RDFDatabase(query.getDbName(), query.getIndexOption());
    db.joinSort(query, 2);
    db.close();

    db = new RDFDatabase(query.getDbName(), query.getIndexOption());
    db.joinSort(query, 3);
    db.close();

    db = new RDFDatabase(query.getDbName(), query.getIndexOption());
    db.joinSort(query, 4);
    db.close();
  }
}
