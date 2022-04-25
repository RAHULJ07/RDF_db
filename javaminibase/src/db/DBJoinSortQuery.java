package db;
import db.ExecutionStrategies.QueryExecutionStrategies;

public class DBJoinSortQuery extends BaseQuery implements IQuery {

    private String queryFile;
    private int numBuf;
    private IndexOption indexOption;

    public DBJoinSortQuery(String dbName,
                           String queryFile,
                           String numBuf,
                           String indexOption) {
        super(dbName);
        this.queryFile = queryFile;
        this.numBuf = Integer.parseInt(numBuf);
        this.indexOption = IndexOption.valueOf(indexOption);
    }

    public String getQueryFile() {
        return queryFile;
    }

    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }

    public int getNumBuf() {
        return numBuf;
    }

    public void setNumBuf(String numBuf) {
        this.numBuf = Integer.valueOf(numBuf);
    }

    public IndexOption getIndexOption() {
        return indexOption;
    }

    public void setIndexOption(String indexOption) {
        this.indexOption = IndexOption.valueOf(indexOption);
    }

    public void execute(int strategy) throws Exception {
        JoinSortQueryFileReader fileReader = new JoinSortQueryFileReader(queryFile);
        JoinSortQuery query = fileReader.getQuery();

        try {
            QueryExecutionStrategies queryExecutionStrategies = new QueryExecutionStrategies(query, numBuf, strategy);
            try {
                queryExecutionStrategies.execute();
            }catch(Exception e){
                System.out.println("Exception in Strategy " + strategy);
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute() throws Exception {

    }
}
