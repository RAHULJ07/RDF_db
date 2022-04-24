package bpiterator;

import basicpattern.BasicPattern;
import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;

/**
 * open a heapfile and according to the condition expression to get
 * output file, call get_next to get all tuples
 */
public class BPFileScan extends BPIterator {
    private Heapfile f;
    private Scan scan;
    private Tuple tuple1;
    public AttrType[] types;
    public short[] s_sizes;
    public int length;

    public BPFileScan(String file_name, int no_fields)
            throws IOException,
            FileScanException {
        tuple1 = new Tuple();

        try {
            f = new Heapfile(file_name);
        } catch (Exception e) {
            throw new FileScanException(e, "Create new heapfile failed");
        }
        length = (no_fields);
        types = new AttrType[(length - 1) * 2 + 1];
        int j = 0;

        for (j = 0; j < (length - 1) * 2; j++) {
            types[j] = new AttrType(AttrType.attrInteger);
        }

        types[j] = new AttrType(AttrType.attrDouble);
        s_sizes = new short[1];
        s_sizes[0] = (short) ((length - 1) * 2 * 4 + 1 * 8);

        try {
            tuple1.setHdr((short) ((length - 1) * 2 + 1), types, s_sizes);
        } catch (InvalidTypeException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (InvalidTupleSizeException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        try {
            scan = f.openScan();
        } catch (Exception e) {
            throw new FileScanException(e, "openScan() failed");
        }
    }

    @Override
    protected int generate_runs(int max_elems, AttrType sortFldType) throws Exception {
        return 0;
    }

    /**
     * @return the result tuple
     * @throws JoinsException                 some join exception
     * @throws IOException                    I/O errors
     * @throws InvalidTupleSizeException      invalid tuple size
     * @throws InvalidTypeException           tuple type not valid
     * @throws PageNotReadException           exception from lower layer
     * @throws PredEvalException              exception from PredEval class
     * @throws UnknowAttrType                 attribute type unknown
     * @throws FieldNumberOutOfBoundException array out of bounds
     * @throws WrongPermat                    exception for wrong FldSpec argument
     */
    public BasicPattern get_next()
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat {
        RID rid = new RID();

        while (true) {
            if ((tuple1 = scan.getNext(rid)) == null) {
                return null;
            }

            tuple1.setHdr((short) ((length - 1) * 2 + 1), types, s_sizes);
            short length1 = (tuple1.noOfFlds());

            BasicPattern bp = new BasicPattern();
            try {
                bp.setHeader((short) ((length1) / 2 + 1));
            } catch (InvalidTupleSizeException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            int i = 0;
            int j = 0;
            for (i = 0, j = 1; i < (length1 / 2); i++) {
                int slotno = tuple1.getIntFld(j++);
                int pageno = tuple1.getIntFld(j++);

                LID lid = new LID(new PageId(pageno), slotno);
                EID eid = lid.returnEID();
                bp.setEIDFld(i + 1, eid);

            }
            double minconf = tuple1.getDoubleFld(j);
            bp.setDoubleFld(i + 1, minconf);
            return bp;
        }
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     */
    public void close() {
        if (!closeFlag) {
            scan.closescan();
            closeFlag = true;
        }
    }
}


