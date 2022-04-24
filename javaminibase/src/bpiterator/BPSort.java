package bpiterator;

import basicpattern.BasicPattern;
import global.AttrType;
import global.BPOrder;
import global.GlobalConst;
import global.PageId;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;

import java.io.IOException;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class BPSort extends BPIterator implements GlobalConst {
    private static final int ARBIT_RUNS = 10;

    private AttrType[] attrTypes;
    private short nCols;
    private short[] strLens;
    private BPFileScan bpFileScan;
    private int sortFld;
    private BPOrder bpOrder;
    private int nPages;
    private byte[][] bufs;
    private boolean firstTime;
    private int Nruns;
    private int maxElemsInHeap;
    private int tupleSize;

    private BPpnodeSplayPQ Q;
    private Heapfile[] tempFiles;
    private int nTempfiles;
    private Tuple outputTuple;
    private int[] nTuples;
    private int nRuns;
    private Tuple opBuf;
    private OBuf oBuf;
    private SpoofIbuf[] ibufs;
    private PageId[] bufsPids;
    private boolean useBM = true; // flag for whether to use buffer manager

    /**
     * Set up for merging the runs.
     * Open an input buffer for each run, and insert the first element (min)
     * from each run into a heap. <code>delete_min() </code> will then get
     * the minimum of all runs.
     *
     * @param tuple_size size (in bytes) of each tuple
     * @param n_R_runs   number of runs
     * @throws IOException     from lower layers
     * @throws LowMemException there is not enough memory to
     *                         sort in two passes (a subclass of BPSortException).
     * @throws BPSortException something went wrong in the lower layer.
     * @throws Exception       other exceptions
     */
    private void setup_for_merge(int tuple_size, int n_R_runs)
            throws Exception {
        if (n_R_runs > nPages)
            throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");

        int i;
        BPpnode cur_node;

        ibufs = new SpoofIbuf[n_R_runs];
        for (int j = 0; j < n_R_runs; j++) {
			ibufs[j] = new SpoofIbuf();
		}

        for (i = 0; i < n_R_runs; i++) {
            byte[][] apage = new byte[1][];
            apage[0] = bufs[i];

            ibufs[i].init(tempFiles[i], apage, 1, tuple_size, nTuples[i]);

            cur_node = new BPpnode();
            cur_node.run_num = i;

            Tuple temp_tuple = new Tuple(tuple_size);

            try {
                temp_tuple.setHdr(nCols, attrTypes, strLens);
            } catch (Exception e) {
                throw new BPSortException(e, "Sort.java: Tuple.setHdr() failed");
            }

            temp_tuple = ibufs[i].Get(temp_tuple);

            if (temp_tuple != null) {
                cur_node.tuple = temp_tuple; // no copy needed
                try {
                    Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (BPUtilsException e) {
                    throw new BPSortException(e, "Sort.java: BasicPatternUtilsException caught from Q.enq()");
                }

            }
        }
        return;
    }

    public void sort_init(AttrType[] in, short len_in, short[] str_sizes)
            throws IOException, BPSortException {
        attrTypes = new AttrType[len_in];
        nCols = len_in;
        int n_strs = 0;

        for (int i = 0; i < len_in; i++) {
            attrTypes[i] = new AttrType(in[i].attrType);

            if (in[i].attrType == AttrType.attrString) {
                n_strs++;
            }
        }

        strLens = new short[n_strs];

        n_strs = 0;
        for (int i = 0; i < len_in; i++) {
            if (attrTypes[i].attrType == AttrType.attrString) {
                strLens[n_strs] = str_sizes[n_strs];
                n_strs++;
            }
        }

        Tuple t = new Tuple(); // need Tuple.java
        try {
            t.setHdr(len_in, attrTypes, str_sizes);
        } catch (Exception e) {
            throw new BPSortException(e, "Sort.java: t.setHdr() failed");
        }
        tupleSize = t.size();

        bufsPids = new PageId[nPages];
        bufs = new byte[nPages][];

        if (useBM) {
            try {
                get_buffer_pages(nPages, bufsPids, bufs);
            } catch (Exception e) {
                throw new BPSortException(e, "Sort.java: BUFmgr error");
            }
        } else {
            for (int k = 0; k < nPages; k++) bufs[k] = new byte[MAX_SPACE];
        }

        tempFiles = new Heapfile[ARBIT_RUNS];
        nTempfiles = ARBIT_RUNS;
        nTuples = new int[ARBIT_RUNS];
        nRuns = ARBIT_RUNS;

        try {
            tempFiles[0] = new Heapfile(null);
        } catch (Exception e) {
            throw new BPSortException(e, "Sort.java: Heapfile error");
        }

        oBuf = new OBuf();

        oBuf.init(bufs, nPages, tupleSize, tempFiles[0], false);

        Q = new BPpnodeSplayPQ(sortFld, in[sortFld - 1], bpOrder);

        opBuf = new Tuple(tupleSize);
        try {
            opBuf.setHdr(nCols, attrTypes, strLens);
        } catch (Exception e) {
            throw new BPSortException(e, "Sort.java: op_buf.setHdr() failed");
        }
    }

    /**
     * Generate sorted runs.
     * Using heap sort.
     *
     * @param max_elems   maximum number of elements in heap
     * @param sortFldType attribute type of the sort field
     * @return number of runs generated
     * @throws IOException     from lower layers
     * @throws BPSortException something went wrong in the lower layer.
     * @throws JoinsException  from <code>Iterator.get_next()</code>
     */
	@Override
	protected int generate_runs(int max_elems, AttrType sortFldType) throws Exception {
        int init_flag = 1;
        BasicPattern bp;
        Tuple tuple;
        BPpnode cur_node;
        BPpnodeSplayPQ Q1 = new BPpnodeSplayPQ(sortFld, sortFldType, bpOrder);
        BPpnodeSplayPQ Q2 = new BPpnodeSplayPQ(sortFld, sortFldType, bpOrder);
        BPpnodeSplayPQ pcurr_Q = Q1;
        BPpnodeSplayPQ pother_Q = Q2;

        int run_num = 0;
        int p_elems_curr_Q = 0;
        int p_elems_other_Q = 0;

        int comp_res;

        // maintain a fixed maximum number of elements in the heap
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
            try {
                bp = bpFileScan.get_next();
                if (bp != null)
                    tuple = bp.getTupleFromBasicPattern();  // according to Iterator.java
                else
                    tuple = null;
                if (init_flag == 1) {
                    AttrType[] in = new AttrType[tuple.noOfFlds()];
                    int j = 0;
                    for (j = 0; j < (tuple.noOfFlds() - 1); j++) {
                        in[j] = new AttrType(AttrType.attrInteger);
                    }
                    in[j] = new AttrType(AttrType.attrDouble);

                    short[] s_sizes = new short[1];
                    s_sizes[0] = (short) ((tuple.noOfFlds() - 1) * 4 + 1 * 8);

                    if (sortFld == -1) {
                        sortFld = tuple.noOfFlds();
                        BPpnodeSplayPQ Q1_confidence = new BPpnodeSplayPQ(sortFld, sortFldType, bpOrder);
                        BPpnodeSplayPQ Q2_confidence = new BPpnodeSplayPQ(sortFld, sortFldType, bpOrder);
                        pcurr_Q = Q1_confidence;
                        pother_Q = Q2_confidence;
                    }

                    sort_init(in, tuple.noOfFlds(), s_sizes);
                    init_flag = 0;
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new BPSortException(e, "Sort.java: get_next() failed");
            }

            if (tuple == null) {
                break;
            }
            cur_node = new BPpnode();
            cur_node.tuple = new Tuple(tuple);

            pcurr_Q.enq(cur_node);
            p_elems_curr_Q++;
        }

        Tuple lastElem = new Tuple(tupleSize);
        try {
            lastElem.setHdr(nCols, attrTypes, strLens);
        } catch (Exception e) {
            throw new BPSortException(e, "Sort.java: setHdr() failed");
        }

        if (bpOrder.bpOrder == BPOrder.Ascending) {
            try {
                MIN_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            } catch (Exception e) {
                throw new BPSortException(e, "MIN_VAL failed");
            }
        } else {
            try {
                MAX_VAL(lastElem, sortFldType);
            } catch (UnknowAttrType e) {
                throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            } catch (Exception e) {
                throw new BPSortException(e, "MIN_VAL failed");
            }
        }

        while (true) {
            cur_node = pcurr_Q.deq();
            if (cur_node == null) break;
            p_elems_curr_Q--;

            comp_res = BPUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, sortFld, lastElem);

            if ((comp_res < 0 && bpOrder.bpOrder == BPOrder.Ascending) || (comp_res > 0 && bpOrder.bpOrder == BPOrder.Descending)) {
                try {
                    pother_Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                }
                p_elems_other_Q++;
            } else {
                BPUtils.SetValue(lastElem, cur_node.tuple, sortFld, sortFldType);
                oBuf.Put(cur_node.tuple);
            }

            if (p_elems_other_Q == max_elems) {
                nTuples[run_num] = (int) oBuf.flush();
                run_num++;

                if (run_num == nTempfiles) {
                    Heapfile[] temp1 = new Heapfile[2 * nTempfiles];
                    for (int i = 0; i < nTempfiles; i++) {
                        temp1[i] = tempFiles[i];
                    }
                    tempFiles = temp1;
                    nTempfiles *= 2;

                    int[] temp2 = new int[2 * nRuns];
                    for (int j = 0; j < nRuns; j++) {
                        temp2[j] = nTuples[j];
                    }
                    nTuples = temp2;
                    nRuns *= 2;
                }

                try {
                    tempFiles[run_num] = new Heapfile(null);
                } catch (Exception e) {
                    throw new BPSortException(e, "Sort.java: create Heapfile failed");
                }

                oBuf.init(bufs, nPages, tupleSize, tempFiles[run_num], false);

                if (bpOrder.bpOrder == BPOrder.Ascending) {
                    try {
                        MIN_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                    } catch (Exception e) {
                        throw new BPSortException(e, "MIN_VAL failed");
                    }
                } else {
                    try {
                        MAX_VAL(lastElem, sortFldType);
                    } catch (UnknowAttrType e) {
                        throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                    } catch (Exception e) {
                        throw new BPSortException(e, "MIN_VAL failed");
                    }
                }

                BPpnodeSplayPQ tempQ = pcurr_Q;
                pcurr_Q = pother_Q;
                pother_Q = tempQ;
                int tempelems = p_elems_curr_Q;
                p_elems_curr_Q = p_elems_other_Q;
                p_elems_other_Q = tempelems;
            } else if (p_elems_curr_Q == 0)
            {
                while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
                    try {
                        bp = bpFileScan.get_next();
                        if (bp != null)
                            tuple = bp.getTupleFromBasicPattern();
                        else
                            tuple = null;
                    } catch (Exception e) {
                        throw new BPSortException(e, "get_next() failed");
                    }

                    if (tuple == null) {
                        break;
                    }
                    cur_node = new BPpnode();
                    cur_node.tuple = new Tuple(tuple); // tuple copy needed --  Bingjie 4/29/98

                    try {
                        pcurr_Q.enq(cur_node);
                    } catch (UnknowAttrType e) {
                        throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                    }
                    p_elems_curr_Q++;
                }
            }

            if (p_elems_curr_Q == 0) {
                if (p_elems_other_Q == 0) {
                    break;
                } else {
                    nTuples[run_num] = (int) oBuf.flush();
                    run_num++;

                    if (run_num == nTempfiles) {
                        Heapfile[] temp1 = new Heapfile[2 * nTempfiles];
                        for (int i = 0; i < nTempfiles; i++) {
                            temp1[i] = tempFiles[i];
                        }
                        tempFiles = temp1;
                        nTempfiles *= 2;

                        int[] temp2 = new int[2 * nRuns];
                        for (int j = 0; j < nRuns; j++) {
                            temp2[j] = nTuples[j];
                        }
                        nTuples = temp2;
                        nRuns *= 2;
                    }

                    try {
                        tempFiles[run_num] = new Heapfile(null);
                    } catch (Exception e) {
                        throw new BPSortException(e, "Sort.java: create Heapfile failed");
                    }

                    oBuf.init(bufs, nPages, tupleSize, tempFiles[run_num], false);

                    if (bpOrder.bpOrder == BPOrder.Ascending) {
                        try {
                            MIN_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                        } catch (Exception e) {
                            throw new BPSortException(e, "MIN_VAL failed");
                        }
                    } else {
                        try {
                            MAX_VAL(lastElem, sortFldType);
                        } catch (UnknowAttrType e) {
                            throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                        } catch (Exception e) {
                            throw new BPSortException(e, "MIN_VAL failed");
                        }
                    }

                    BPpnodeSplayPQ tempQ = pcurr_Q;
                    pcurr_Q = pother_Q;
                    pother_Q = tempQ;
                    int tempelems = p_elems_curr_Q;
                    p_elems_curr_Q = p_elems_other_Q;
                    p_elems_other_Q = tempelems;
                }
            }
        }

        // close the last run
        nTuples[run_num] = (int) oBuf.flush();
        run_num++;
        return run_num;
    }

    /**
     * Remove the minimum value among all the runs.
     *
     * @return the minimum tuple removed
     * @throws IOException     from lower layers
     * @throws BPSortException something went wrong in the lower layer.
     */
    private Tuple delete_min() throws Exception {
        BPpnode cur_node;
        Tuple new_tuple, old_tuple;

        cur_node = Q.deq();
        old_tuple = cur_node.tuple;

        if (ibufs[cur_node.run_num].empty() != true) {

			new_tuple = new Tuple(tupleSize);
            try {
                new_tuple.setHdr(nCols, attrTypes, strLens);
            } catch (Exception e) {
                throw new BPSortException(e, "Sort.java: setHdr() failed");
            }

            new_tuple = ibufs[cur_node.run_num].Get(new_tuple);
            if (new_tuple != null) {

                cur_node.tuple = new_tuple;
                try {
                    Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (BPUtilsException e) {
                    throw new BPSortException(e, "Sort.java: BasicPatternUtilsException caught from Q.enq()");
                }
            } else {
                throw new BPSortException("********** Wait a minute, I thought input is not empty ***************");
            }

        }

        return old_tuple;
    }

    /**
     * Set lastElem to be the minimum value of the appropriate type
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MIN_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                lastElem.setIntFld(sortFld, Integer.MIN_VALUE);
                lastElem.setIntFld(sortFld + 1, Integer.MIN_VALUE);
                break;
            case AttrType.attrDouble:
                lastElem.setDoubleFld(sortFld, Double.MIN_VALUE);
                break;
            default:
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        return;
    }

    /**
     * Set lastElem to be the maximum value of the appropriate type
     *
     * @param lastElem    the tuple
     * @param sortFldType the sort field type
     * @throws IOException    from lower layers
     * @throws UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MAX_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                lastElem.setIntFld(sortFld, Integer.MAX_VALUE);
                lastElem.setIntFld(sortFld + 1, Integer.MAX_VALUE);
                break;
            case AttrType.attrDouble:
                lastElem.setDoubleFld(sortFld, Double.MAX_VALUE);
                break;
            default:
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        return;
    }

    /**
     * Class constructor: set up the sorting basic patterns
     *
     * @param am         an iterator for accessing the basicpatterns
     * @param sort_order the sorting order (ASCENDING, DESCENDING)
     * @param sort_fld   the field number of the field to sort on ( -1 on confidence )
     * @param n_pages    amount of memory (in pages) available for sorting
     * @throws IOException     from lower layers
     * @throws BPSortException something went wrong in the lower layer.
     */
    public BPSort(BPFileScan am, BPOrder sort_order, int sort_fld, int n_pages) {
        bpFileScan = am;
        bpOrder = sort_order;
        nPages = n_pages;
        sortFld = sort_fld;

        if (sort_fld != -1) {
            sortFld = sort_fld * 2 - 1;
        }

        firstTime = true;
        maxElemsInHeap = 200;
    }

    /**
     * Returns the next basicpattern in sorted order.
     * Note: You need to copy out the content of the basicpattern, otherwise it
     * will be overwritten by the next <code>get_next()</code> call.
     *
     * @return the next tuple, null if all basicpatterns exhausted
     * @throws IOException     from lower layers
     * @throws BPSortException something went wrong in the lower layer.
     * @throws JoinsException  from <code>generate_runs()</code>.
     * @throws UnknowAttrType  attribute type unknown
     * @throws LowMemException memory low exception
     * @throws Exception       other exceptions
     */
    public BasicPattern get_next()
            throws IOException,
            BPSortException,
            UnknowAttrType,
            LowMemException,
            JoinsException,
            Exception {
        if (firstTime) {
            firstTime = false;

            AttrType sortFldTyp;
            if (sortFld < 0) sortFldTyp = new AttrType(AttrType.attrDouble);
            else sortFldTyp = new AttrType(AttrType.attrInteger);
            Nruns = generate_runs(maxElemsInHeap, sortFldTyp);
            setup_for_merge(tupleSize, Nruns);
        }

        if (Q.empty()) {
            return null;
        }

        outputTuple = delete_min();
        if (outputTuple != null) {
            opBuf.tupleCopy(outputTuple);
            BasicPattern bpnew = new BasicPattern(outputTuple);
            return bpnew;
        } else {
            return null;
        }
    }

    /**
     * Cleaning up, including releasing buffer pages from the buffer pool
     * and removing temporary files from the database.
     *
     * @throws IOException from lower layers
     */
    public void close() throws IOException {
        // clean up
        if (!closeFlag) {

            try {
                bpFileScan.close();
            } catch (Exception e) {
                try {
                    throw new BPSortException(e, "Sort.java: error in closing iterator.");
                } catch (BPSortException e1) {
                    e1.printStackTrace();
                }
            }

            if (useBM) {
                try {
                    free_buffer_pages(nPages, bufsPids);
                } catch (Exception e) {
                    try {
                        throw new BPSortException(e, "Sort.java: BUFmgr error");
                    } catch (BPSortException e1) {
                        e1.printStackTrace();
                    }
                }
                for (int i = 0; i < nPages; i++) bufsPids[i].pid = INVALID_PAGE;
            }

            for (int i = 0; i < tempFiles.length; i++) {
                if (tempFiles[i] != null) {
                    try {
                        tempFiles[i].deleteFile();
                    } catch (Exception e) {
                        try {
                            throw new BPSortException(e, "Sort.java: Heapfile error");
                        } catch (BPSortException e1) {
                            e1.printStackTrace();
                        }
                    }
                    tempFiles[i] = null;
                }
            }
            closeFlag = true;
        }
    }
}


