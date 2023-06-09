package bpiterator;

import basicpattern.BasicPattern;
import bufmgr.PageNotReadException;
import diskmgr.Page;
import global.AttrType;
import global.Flags;
import global.PageId;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import iterator.*;

import java.io.IOException;

/**
 * Relational Operators and access methods are iterators.
 */
public abstract class BPIterator implements Flags {

    /**
     * a flag to indicate whether this iterator has been closed.
     * it is set to true the first time the <code>close()</code>
     * function is called.
     * multiple calls to the <code>close()</code> function will
     * not be a problem.
     */
    public boolean closeFlag = false;

    protected abstract int generate_runs(int max_elems, AttrType sortFldType)
            throws Exception;

    /**
     * abstract method, every subclass must implement it.
     *
     * @return the result BasicPattern
     * @throws IOException               I/O errors
     * @throws InvalidTupleSizeException invalid tuple size
     * @throws InvalidTypeException      tuple type not valid
     * @throws PageNotReadException      exception from lower layer
     * @throws TupleUtilsException       exception from using tuple utilities
     * @throws SortException             sort exception
     * @throws LowMemException           memory error
     * @throws UnknowAttrType            attribute type unknown
     * @throws UnknownKeyTypeException   key type unknown
     * @throws Exception                 other exceptions
     */
    public abstract BasicPattern get_next()
            throws Exception;

    /**
     * Closes the iterator
     *
     * @throws IOException    I/O errors
     * @throws JoinsException some join exception
     * @throws SortException  exception Sort class
     */
    public abstract void close()
            throws IOException,
            SortException;

    /**
     * tries to get num_pages of buffer space
     *
     * @param num_pages the number of pages
     * @param pageIds   the corresponding PageId for each page
     * @param bufs      the buffer space
     * @throws IteratorBMException exceptions from bufmgr layer
     */
    public void get_buffer_pages(int num_pages, PageId[] pageIds, byte[][] bufs)
            throws IteratorBMException {
        Page pgptr = new Page();
        PageId pgid = null;

        for (int i = 0; i < num_pages; i++) {
            pgptr.setpage(bufs[i]);

            pgid = newPage(pgptr, 1);
            pageIds[i] = new PageId(pgid.pid);

            bufs[i] = pgptr.getpage();

        }
    }

    /**
     * Free all the buffer pages we requested earlier.
     * should be called in the destructor
     *
     * @param n_pages the number of pages
     * @param pageIds the corresponding PageId for each page
     * @throws IteratorBMException exception from bufmgr class
     */
    public void free_buffer_pages(int n_pages, PageId[] pageIds) throws IteratorBMException {
        for (int i = 0; i < n_pages; i++) {
            freePage(pageIds[i]);
        }
    }

    private void freePage(PageId pageno) throws IteratorBMException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            throw new IteratorBMException(e, "Iterator.java: freePage() failed");
        }
    }

    /**
     * Create a new page
     *
     * @param page page
     * @param num  number integer
     * @return PageId
     * @throws IteratorBMException
     */
    private PageId newPage(Page page, int num) throws IteratorBMException {

		PageId tmpId = new PageId();
        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new IteratorBMException(e, "Iterator.java: newPage() failed");
        }

        return tmpId;
    }
}
