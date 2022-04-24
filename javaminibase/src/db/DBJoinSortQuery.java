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
            //Strategy 1
            try{
                iQueryExecutionStrategies.execute(InnerJoinOption.HeapScan, InnerJoinOption.HeapScan);
            }catch(Exception e){
                System.out.println("Exception in Strategy 1");
                e.printStackTrace();
            }
            //Strategy 2
            try {
                iQueryExecutionStrategies.execute(InnerJoinOption.HeapScan, InnerJoinOption.IndexScan);
            }catch(Exception e){
                System.out.println("Exception in Strategy 2");
                e.printStackTrace();
            }
            //Strategy 3
            try{
                iQueryExecutionStrategies.execute(InnerJoinOption.IndexScan, InnerJoinOption.HeapScan);
            }catch(Exception e){
                System.out.println("Exception in Strategy 3");
                e.printStackTrace();
            }
            //Strategy 4
            try{
                iQueryExecutionStrategies.execute(InnerJoinOption.IndexScan, InnerJoinOption.IndexScan);
            }catch(Exception e){
                System.out.println("Exception in Strategy 4");
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
