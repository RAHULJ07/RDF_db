package basicpattern;

import static global.RDFSystemDefs.initRdfDB;

import diskmgr.rdf.RdfDB;
import global.AttrType;
import global.Convert;
import global.EID;
import global.GlobalConst;
import global.LID;
import global.PageId;
import global.RDFSystemDefs;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Label;
import heap.Tuple;
import heap.labelheap.LabelHeapFile;
import java.io.IOException;


public class BasicPattern implements GlobalConst {

  /**
   * Maximum size of any BasicPattern
   */
  public static final int max_size = MINIBASE_PAGESIZE;

  /**
   * a byte array to hold data
   */
  private byte[] data;

  /**
   * start position of this BasicPattern in data[]
   */
  private int basicPattern_offset;

  /**
   * length of this BasicPattern
   */
  private int basicPattern_length;

  /**
   * private field
   * Number of fields in this BasicPattern
   */
  private short fldCnt;

  /**
   * private field
   * Array of offsets of the fields
   */

  private short[] fldOffset;

  public static RDFSystemDefs sysdef = null;

  /**
   * Class constructor
   * Creat a new BasicPattern with length = max_size,tuple offset = 0.
   */
  public BasicPattern() {
    // Creat a new BasicPattern
    data = new byte[max_size];
    basicPattern_offset = 0;
    basicPattern_length = max_size;
  }

  /**
   * BasicPattern Constructor
   * @param abasicpattern a byte array which contains the basicpattern
   * @param offset the offset of the basicpattern in the byte array
   * @param length the length of the basicpattern
   */
  public BasicPattern(byte[] abasicpattern, int offset, int length) {
    data = abasicpattern;
    basicPattern_offset = offset;
    basicPattern_length = length;
    //  fldCnt = getShortValue(offset, data);
  }

  /**
   * Constructor(used as BasicPattern copy)
   *
   * @param fromBasicPattern a byte array which contains the BasicPattern
   */
  public BasicPattern(BasicPattern fromBasicPattern) {
    data = fromBasicPattern.getBasicPatternByteArray();
    basicPattern_length = fromBasicPattern.getLength();
    basicPattern_offset = 0;
    fldCnt = fromBasicPattern.noOfFlds();
    fldOffset = fromBasicPattern.copyFldOffset();
  }

  /**
   * Class constructor
   * Creat a new BasicPattern with length = size,BasicPattern offset = 0.
   */

  public BasicPattern(int size) {
    // Creat a new BasicPattern
    data = new byte[size];
    basicPattern_offset = 0;
    basicPattern_length = size;
  }

  /**
   * Copy a BasicPattern to the current BasicPattern position
   * you must make sure the BasicPattern lengths must be equal
   *
   * @param fromBasicPattern the BasicPattern being copied
   */
  public void basicPatternCopy(BasicPattern fromBasicPattern) {
    byte[] temparray = fromBasicPattern.getBasicPatternByteArray();
    System.arraycopy(temparray, 0, data, basicPattern_offset, basicPattern_length);
    //       fldCnt = fromTuple.noOfFlds();
    //       fldOffset = fromTuple.copyFldOffset();
  }

  /**
   * This is used when you don't want to use the constructor
   *
   * @param abasicpattern a byte array which contains the BasicPattern
   * @param offset the offset of the BasicPattern in the byte array
   * @param length the length of the BasicPattern
   */

  public void basicPatternInit(byte[] abasicpattern, int offset, int length) {
    data = abasicpattern;
    basicPattern_offset = offset;
    basicPattern_length = length;
  }

  /**
   * Set a BasicPattern with the given BasicPattern length and offset
   *
   * @param record a byte array contains the BasicPattern
   * @param offset the offset of the BasicPattern ( =0 by default)
   * @param length the length of the BasicPattern
   */
  public void basicPatternSet(byte[] record, int offset, int length) {
    System.arraycopy(record, offset, data, 0, length);
    basicPattern_offset = 0;
    basicPattern_length = length;
  }

  /**
   * get the length of a BasicPattern, call this method if you did not
   * call setHdr () before
   *
   * @return length of this BasicPattern in bytes
   */
  public int getLength() {
    return basicPattern_length;
  }

  /**
   * get the length of a BasicPattern, call this method if you did
   * call setHdr () before
   *
   * @return size of this BasicPattern in bytes
   */
  public short size() {
    return ((short) (fldOffset[fldCnt] - basicPattern_offset));
  }

  /**
   * get the offset of a BasicPattern
   *
   * @return offset of the BasicPattern in byte array
   */
  public int getOffset() {
    return basicPattern_offset;
  }

  /**
   * Copy the BasicPattern byte array out
   *
   * @return byte[], a byte array contains the BasicPattern
   * the length of byte[] = length of the BasicPattern
   */

  public byte[] getBasicPatternByteArray() {
    byte[] tuplecopy = new byte[basicPattern_length];
    System.arraycopy(data, basicPattern_offset, tuplecopy, 0, basicPattern_length);
    return tuplecopy;
  }


  public Tuple getTuplefromBasicPattern()
  {
    Tuple tuple1 = new Tuple();
    int length = (fldCnt);
    AttrType[]	 types = new AttrType[(length-1)*2 +1];
    int j = 0;
    for(j = 0 ; j < (length-1)*2  ; j++)
    {
      types[j] = new AttrType(AttrType.attrInteger);
    }
    types[j] = new AttrType(AttrType.attrDouble);
    short[] s_sizes = new short[1];
    s_sizes[0] = (short)((length-1)*2 * 4 + 1* 8);
    try {
      tuple1.setHdr((short)((length-1)*2 +1) , types, s_sizes);
    } catch (InvalidTypeException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (InvalidTupleSizeException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    int i = 0;
    j = 1;
    for( i = 0 ; i < fldCnt-1 ; i++)
    {
      try {
        EID eid = getEIDFld(i+1);
        tuple1.setIntFld(j++, eid.getSlotNo());
        tuple1.setIntFld(j++, eid.getPageNo().pid);
      } catch (FieldNumberOutOfBoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    try {
      tuple1.setDoubleFld(j,getDoubleFld(fldCnt));
    } catch (FieldNumberOutOfBoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
	   /*	  	try {
			tuple1.print(types);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    return tuple1;
  }


  public BasicPattern getBasicPatternfromTuple(Tuple atuple)
      throws FieldNumberOutOfBoundException, IOException {

    short length = (atuple.noOfFlds());

    BasicPattern bp = new BasicPattern();
    try {
      bp.setHdr((short)((length)/2 + 1));
    }  catch (InvalidTupleSizeException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    int i = 0;
    int j = 0;
    for(i = 0 , j = 1; i < (length/2)  ; i++)
    {
      int slotno = atuple.getIntFld(j++);
      int pageno = atuple.getIntFld(j++);

      LID lid = new LID(new PageId(pageno),slotno);
      EID eid = lid.getEntityID();
      bp.setEIDFld(i+1, eid);

    }
    double minConfidence = atuple.getDoubleFld(j);
    bp.setDoubleFld(i+1, minConfidence);
    return bp;

  }


  /**
   * return the data byte array
   *
   * @return data byte array
   */

  public byte[] returnBasicPatternByteArray() {
    return data;
  }

  /**
   * Get pageno and slotno and Convert this field into EID
   *
   * @param	fldNo	the field number
   * @return		the converted eid if success
   *
   * @exception IOException I/O errors
   * @exception FieldNumberOutOfBoundException BasicPattern field number out of bound
   */

  public EID getEIDFld(int fldNo)
      throws IOException, FieldNumberOutOfBoundException
  {
    int pageno, slotno;
    if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
      pageno = Convert.getIntValue(fldOffset[fldNo -1], data);
      slotno = Convert.getIntValue(fldOffset[fldNo -1] + 4, data);
      PageId page = new PageId();
      page.pid = pageno;
      LID lid = new LID(page,slotno);
      EID eid = new EID(lid);
      return eid;
    }
    else
      throw new FieldNumberOutOfBoundException (null, "BP:BASICPATTERN_FIELDNO_OUT_OF_BOUND");
  }


  /**
   * Convert this field in to double
   *
   * @param    fldNo   the field number
   * @return           the converted double number  if success
   *
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException BasicPattern field number out of bound
   */

  public double getDoubleFld(int fldNo)
      throws IOException, FieldNumberOutOfBoundException
  {
    double val;
    if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
      val = Convert.getDoubleValue(fldOffset[fldNo -1], data);
      return val;
    }
    else
      throw new FieldNumberOutOfBoundException (null, "TUPLE:BASICPATTERN_FLDNO_OUT_OF_BOUND");
  }

  /**
   * Set this field to EID value
   *
   * @param	fldNo	the field number
   * @param	val	the EID value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException BasicPattern field number out of bound
   */

  public BasicPattern setEIDFld(int fldNo, EID val)
      throws IOException, FieldNumberOutOfBoundException
  {
    if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
      Convert.setIntValue (val.getPageNo().pid, fldOffset[fldNo -1], data);
      Convert.setIntValue (val.getSlotNo(), fldOffset[fldNo -1]+4, data);
      return this;
    }
    else
      throw new FieldNumberOutOfBoundException (null, "BP :BASIC_PATTERN_FLDNO_OUT_OF_BOUND");
  }

  /**
   * Set this field to double value
   *
   * @param     fldNo   the field number
   * @param     val     the double value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException BasicPattern field number out of bound
   */

  public BasicPattern setDoubleFld(int fldNo, double val)
      throws IOException, FieldNumberOutOfBoundException
  {
    if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
      Convert.setDoubleValue (val, fldOffset[fldNo -1], data);
      return this;
    }
    else
      throw new FieldNumberOutOfBoundException (null, "BasicPattern:BASIC PATTERN_FLDNO_OUT_OF_BOUND");

  }

  /**
   * setHdr will set the header of this BasicPattern.
   *
   * @param	numFlds	  number of nodeIds + 1 (for confidence)
   *
   * @exception IOException I/O errors
   * @exception InvalidTypeException Invalid BasicPattern type
   * @exception InvalidTupleSizeException BasicPattern size too big
   *
   */

  public void setHdr (short numFlds) throws InvalidTupleSizeException, IOException
  {
    if((numFlds +2)*2 > max_size)
      throw new InvalidTupleSizeException (null, "BP: BASIC PATTERN_TOOBIG_ERROR");

    fldCnt = numFlds;
    Convert.setShortValue(numFlds, basicPattern_offset, data);
    fldOffset = new short[numFlds+1];
    int pos = basicPattern_offset+2;  // start position for fldOffset[]


    fldOffset[0] = (short) ((numFlds +2) * 2 + basicPattern_offset);

    Convert.setShortValue(fldOffset[0], pos, data);
    pos +=2;
    short strCount =0;
    short incr;
    int i;

    for (i=1; i<numFlds; i++)
    {
      incr = 8;
      fldOffset[i]  = (short) (fldOffset[i-1] + incr);
      Convert.setShortValue(fldOffset[i], pos, data);
      pos +=2;

    }

    // For confidence
    incr = 8;

    fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
    Convert.setShortValue(fldOffset[numFlds], pos, data);

    basicPattern_length = fldOffset[numFlds] - basicPattern_offset;

    if(basicPattern_length > max_size)
      throw new InvalidTupleSizeException (null, "BP: BASIC PATTERN_TOOBIG_ERROR");
  }


  /**
   * Returns number of fields in this tuple
   *
   * @return the number of fields in this tuple
   *
   */

  public short noOfFlds()
  {
    return fldCnt;
  }

  /**
   * Makes a copy of the fldOffset array
   *
   * @return a copy of the fldOffset arrray
   *
   */

  public short[] copyFldOffset()
  {
    short[] newFldOffset = new short[fldCnt + 1];
    for (int i=0; i<=fldCnt; i++) {
      newFldOffset[i] = fldOffset[i];
    }

    return newFldOffset;
  }

  /**
   * Print out the basic pattern
   * @Exception IOException I/O exception
   */
  public void print()
      throws IOException
  {
    int val;
    double doubleVal;
    String stringVal;
    initRdfDB();
    LabelHeapFile Entity_HF = ((RdfDB)SystemDefs.JavabaseDB).getEntityHeapFile();
    System.out.print("[");
    try {
      for(int i = 1 ; i <= fldCnt -1 ; i++)
      {
        Label subject = Entity_HF.getLabel(this.getEIDFld(i).returnLID());
        System.out.printf("%30s  ",subject.getLabel());
      }
      System.out.print(getDoubleFld(fldCnt));
      System.out.println("]");

    } catch (InvalidSlotNumberException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvalidTupleSizeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HFException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HFDiskMgrException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (HFBufMgrException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (FieldNumberOutOfBoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }


  public void printIDs()
  {
    int val;
    double dval;
    String sval;
    System.out.print("[");
    try {
      for(int i = 1 ; i <= fldCnt -1 ; i++)
      {
        System.out.print("(" + this.getEIDFld(i).getPageNo().pid + "," + this.getEIDFld(i).getSlotNo() + ")");
      }
      System.out.print("Confidence:: "+getDoubleFld(fldCnt));
      System.out.println("]");

    } catch (Exception e) {
      System.out.println("Error printing BP"+e);
    }
  }


  public boolean findEID(EID eid)
  {
    boolean found = false;
    try
    {
      EID e = null;
      for (int i=1; i<= fldCnt-1; i++)
      {
        if(eid.equals(getEIDFld(i)))
        {
          found = true;
          break;
        }

      }
    }
    catch(Exception e)
    {
      System.out.print(e);

    }
    return found;
  }


}
