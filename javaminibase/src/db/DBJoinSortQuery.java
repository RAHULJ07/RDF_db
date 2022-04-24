package db;

import basicpattern.BPQuadHeapJoin;
import basicpattern.BPQuadIndexJoin;
import basicpattern.BasicPattern;
import bpiterator.BPFileScan;
import bpiterator.BPSort;
import db.ExecutionStrategies.IQueryExecutionStrategies;
import db.ExecutionStrategies.QueryExecutionStrategies;
import diskmgr.rdf.RdfDB;
import diskmgr.rdf.TStream;
import global.BPOrder;
import global.QuadOrder;
import global.RDFSystemDefs;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;

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

    @Override
    public void execute() throws Exception {
        JoinSortQueryFileReader fileReader = new JoinSortQueryFileReader(queryFile);
        JoinSortQuery query = fileReader.getQuery();


        try {
            IQueryExecutionStrategies iQueryExecutionStrategies = new QueryExecutionStrategies(query, numBuf);
            iQueryExecutionStrategies.executeHeapfileHeapfileStrategy();
            iQueryExecutionStrategies.executeIndexHeapfileStrategy();
            iQueryExecutionStrategies.executeHeapfileIndexStrategy();
            iQueryExecutionStrategies.executeIndexIndexStrategy();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
