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
  public static final int maxSize = MINIBASE_PAGESIZE;
  public static RDFSystemDefs sysdef = null;
  /**
   * a byte array to hold data
   */
  private byte[] data;
  /**
   * start position of this BasicPattern in data[]
   */
  private int basicPatternOffset;
  /**
   * length of this BasicPattern
   */
  private int basicPatternLength;
  /**
   * private field Number of fields in this BasicPattern
   */
  private short fieldCount;
  /**
   * private field Array of offsets of the fields
   */

  private short[] fieldOffset;

  /**
   * Class constructor Creat a new BasicPattern with length = max_size,tuple offset = 0.
   */
  public BasicPattern() {
    // Creat a new BasicPattern
    data = new byte[maxSize];
    basicPatternOffset = 0;
    basicPatternLength = maxSize;
  }

  /**
   * BasicPattern Constructor
   *
   * @param abasicpattern a byte array which contains the basicpattern
   * @param offset        the offset of the basicpattern in the byte array
   * @param length        the length of the basicpattern
   */
  public BasicPattern(byte[] abasicpattern, int offset, int length) {
    data = abasicpattern;
    basicPatternOffset = offset;
    basicPatternLength = length;
  }

  /**
   * Constructor(used as BasicPattern copy)
   *
   * @param fromBasicPattern a byte array which contains the BasicPattern
   */
  public BasicPattern(BasicPattern fromBasicPattern) {
    data = fromBasicPattern.getBasicPatternByteArray();
    basicPatternLength = fromBasicPattern.getLength();
    basicPatternOffset = 0;
    fieldCount = fromBasicPattern.numberOfFlds();
    fieldOffset = fromBasicPattern.copyFieldOffset();
  }

  /**
   * Class constructor Creat a new BasicPattern with length = size,BasicPattern offset = 0.
   */

  public BasicPattern(int size) {
    // Creat a new BasicPattern
    data = new byte[size];
    basicPatternOffset = 0;
    basicPatternLength = size;
  }

  /**
   * Copy a BasicPattern to the current BasicPattern position you must make sure the BasicPattern
   * lengths must be equal
   *
   * @param fromBasicPattern the BasicPattern being copied
   */
  public void basicPatternCopy(BasicPattern fromBasicPattern) {
    byte[] temparray = fromBasicPattern.getBasicPatternByteArray();
    System.arraycopy(temparray, 0, data, basicPatternOffset, basicPatternLength);
  }

  /**
   * This is used when you don't want to use the constructor
   *
   * @param abasicpattern a byte array which contains the BasicPattern
   * @param offset        the offset of the BasicPattern in the byte array
   * @param length        the length of the BasicPattern
   */

  public void basicPatternInit(byte[] abasicpattern, int offset, int length) {
    data = abasicpattern;
    basicPatternOffset = offset;
    basicPatternLength = length;
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
    basicPatternOffset = 0;
    basicPatternLength = length;
  }

  /**
   * get the length of a BasicPattern, call this method if you did not call setHdr () before
   *
   * @return length of this BasicPattern in bytes
   */
  public int getLength() {
    return basicPatternLength;
  }

  /**
   * get the length of a BasicPattern, call this method if you did call setHdr () before
   *
   * @return size of this BasicPattern in bytes
   */
  public short size() {
    return ((short) (fieldOffset[fieldCount] - basicPatternOffset));
  }

  /**
   * get the offset of a BasicPattern
   *
   * @return offset of the BasicPattern in byte array
   */
  public int getOffset() {
    return basicPatternOffset;
  }

  /**
   * Copy the BasicPattern byte array out
   *
   * @return byte[], a byte array contains the BasicPattern the length of byte[] = length of the
   * BasicPattern
   */

  public byte[] getBasicPatternByteArray() {
    byte[] tupleCopy = new byte[basicPatternLength];
    System.arraycopy(data, basicPatternOffset, tupleCopy, 0, basicPatternLength);
    return tupleCopy;
  }


  public Tuple getTuplefromBasicPattern() {
    Tuple tuple = new Tuple();
    int length = (fieldCount);
    AttrType[] types = new AttrType[(length - 1) * 2 + 1];
    int j = 0;
    for (j = 0; j < (length - 1) * 2; j++) {
      types[j] = new AttrType(AttrType.attrInteger);
    }
    types[j] = new AttrType(AttrType.attrDouble);
    short[] s_sizes = new short[1];
    s_sizes[0] = (short) ((length - 1) * 2 * 4 + 1 * 8);
    try {
      tuple.setHdr((short) ((length - 1) * 2 + 1), types, s_sizes);
    } catch (InvalidTypeException e1) {
      e1.printStackTrace();
    } catch (InvalidTupleSizeException e1) {
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    int i = 0;
    j = 1;
    for (i = 0; i < fieldCount - 1; i++) {
      try {
        EID eid = getEIDField(i + 1);
        tuple.setIntFld(j++, eid.getSlotNo());
        tuple.setIntFld(j++, eid.getPageNo().pid);
      } catch (FieldNumberOutOfBoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      tuple.setDoubleFld(j, getDoubleFld(fieldCount));
    } catch (FieldNumberOutOfBoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return tuple;
  }


  public BasicPattern getBasicPatternfromTuple(Tuple tuple)
      throws FieldNumberOutOfBoundException, IOException, InvalidTupleSizeException, InvalidTypeException {

    short length = (tuple.noOfFlds());

    AttrType[] types = new AttrType[(length - 1) * 2 + 1];
    int j = 0;
    for (j = 0; j < (length - 1) * 2; j++) {
      types[j] = new AttrType(AttrType.attrInteger);
    }
    types[j] = new AttrType(AttrType.attrDouble);
    short[] s_sizes = new short[1];
    s_sizes[0] = (short) ((length - 1) * 2 * 4 + 1 * 8);

    tuple.setHdr(length, types, s_sizes);

    BasicPattern bp = new BasicPattern();
    try {
      bp.setHdr((short) ((length) / 2 + 1));
    } catch (InvalidTupleSizeException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    int i = 0;
    j = 0;
    for (i = 0, j = 1; i < (length / 2); i++) {
      int slotno = tuple.getIntFld(j++);
      int pageno = tuple.getIntFld(j++);

      LID lid = new LID(new PageId(pageno), slotno);
      EID eid = lid.getEntityID();
      bp.setEIDField(i + 1, eid);

    }
    double minConfidence = tuple.getDoubleFld(j);
    bp.setDoubleFld(i + 1, minConfidence);
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
   * @throws IOException                    I/O errors
   * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound
   * @param  fieldNo  the field number
   * @return the converted eid if success
   */

  public EID getEIDField(int fieldNo)
      throws IOException, FieldNumberOutOfBoundException {
    int pageno, slotno;
    if ((fieldNo > 0) && (fieldNo <= fieldCount)) {
      pageno = Convert.getIntValue(fieldOffset[fieldNo - 1], data);
      slotno = Convert.getIntValue(fieldOffset[fieldNo - 1] + 4, data);
      PageId page = new PageId();
      page.pid = pageno;
      LID lid = new LID(page, slotno);
      EID eid = new EID(lid);
      return eid;
    } else {
      throw new FieldNumberOutOfBoundException(null, "BP:BASICPATTERN_FIELDNO_OUT_OF_BOUND");
    }
  }


  /**
   * Convert this field in to double
   *
   * @param fieldNo the field number
   * @return the converted double number  if success
   * @throws IOException                    I/O errors
   * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound
   */

  public double getDoubleFld(int fieldNo)
      throws IOException, FieldNumberOutOfBoundException {
    double val;
    if ((fieldNo > 0) && (fieldNo <= fieldCount)) {
      val = Convert.getDoubleValue(fieldOffset[fieldNo - 1], data);
      return val;
    } else {
      throw new FieldNumberOutOfBoundException(null, "TUPLE:BASICPATTERN_FLDNO_OUT_OF_BOUND");
    }
  }

  /**
   * Set this field to EID value
   *
   * @throws IOException                    I/O errors
   * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound
   * @param  fieldNo  the field number
   * @param  val  the EID value
   */

  public BasicPattern setEIDField(int fieldNo, EID val)
      throws IOException, FieldNumberOutOfBoundException {
    if ((fieldNo > 0) && (fieldNo <= fieldCount)) {
      Convert.setIntValue(val.getPageNo().pid, fieldOffset[fieldNo - 1], data);
      Convert.setIntValue(val.getSlotNo(), fieldOffset[fieldNo - 1] + 4, data);
      return this;
    } else {
      throw new FieldNumberOutOfBoundException(null, "BP :BASIC_PATTERN_FLDNO_OUT_OF_BOUND");
    }
  }

  /**
   * Set this field to double value
   *
   * @param fieldNo the field number
   * @param val     the double value
   * @throws IOException                    I/O errors
   * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound
   */

  public BasicPattern setDoubleFld(int fieldNo, double val)
      throws IOException, FieldNumberOutOfBoundException {
    if ((fieldNo > 0) && (fieldNo <= fieldCount)) {
      Convert.setDoubleValue(val, fieldOffset[fieldNo - 1], data);
      return this;
    } else {
      throw new FieldNumberOutOfBoundException(null,
          "BasicPattern:BASIC PATTERN_FLDNO_OUT_OF_BOUND");
    }

  }

  /**
   * setHdr will set the header of this BasicPattern.
   *
   * @throws IOException               I/O errors
   * @throws InvalidTypeException      Invalid BasicPattern type
   * @throws InvalidTupleSizeException BasicPattern size too big
   * @param  numberOfFields   number of nodeIds + 1 (for confidence)
   */

  public void setHdr(short numberOfFields) throws InvalidTupleSizeException, IOException {
    if ((numberOfFields + 2) * 2 > maxSize) {
      throw new InvalidTupleSizeException(null, "BP: BASIC PATTERN_TOOBIG_ERROR");
    }

    fieldCount = numberOfFields;
    Convert.setShortValue(numberOfFields, basicPatternOffset, data);
    fieldOffset = new short[numberOfFields + 1];
    int pos = basicPatternOffset + 2;  // start position for fldOffset[]

    fieldOffset[0] = (short) ((numberOfFields + 2) * 2 + basicPatternOffset);

    Convert.setShortValue(fieldOffset[0], pos, data);
    pos += 2;
    short strCount = 0;
    short increment;
    int i;

    for (i = 1; i < numberOfFields; i++) {
      increment = 8;
      fieldOffset[i] = (short) (fieldOffset[i - 1] + increment);
      Convert.setShortValue(fieldOffset[i], pos, data);
      pos += 2;

    }

    // For confidence
    increment = 8;

    fieldOffset[numberOfFields] = (short) (fieldOffset[i - 1] + increment);
    Convert.setShortValue(fieldOffset[numberOfFields], pos, data);

    basicPatternLength = fieldOffset[numberOfFields] - basicPatternOffset;

    if (basicPatternLength > maxSize) {
      throw new InvalidTupleSizeException(null, "BP: BASIC PATTERN_TOOBIG_ERROR");
    }
  }


  /**
   * Returns number of fields in this tuple
   *
   * @return the number of fields in this tuple
   */

  public short numberOfFlds() {
    return fieldCount;
  }

  /**
   * Makes a copy of the fldOffset array
   *
   * @return a copy of the fldOffset arrray
   */

  public short[] copyFieldOffset() {
    short[] newFieldOffset = new short[fieldCount + 1];
    for (int i = 0; i <= fieldCount; i++) {
      newFieldOffset[i] = fieldOffset[i];
    }

    return newFieldOffset;
  }

  /**
   * Print out the basic pattern
   *
   * @Exception IOException I/O exception
   */
  public void print()
      throws IOException {
    initRdfDB();
    LabelHeapFile Entity_HF = ((RdfDB) SystemDefs.JavabaseDB).getEntityHeapFile();
    System.out.print("[");
    try {
      for (int i = 1; i <= fieldCount - 1; i++) {
        Label subject = Entity_HF.getLabel(this.getEIDField(i).returnLID());
        System.out.printf("%30s  ", subject.getLabel());
      }
      System.out.print(getDoubleFld(fieldCount));
      System.out.println("]");

    } catch (InvalidSlotNumberException e) {
      e.printStackTrace();
    } catch (InvalidTupleSizeException e) {
      e.printStackTrace();
    } catch (HFException e) {
      e.printStackTrace();
    } catch (HFDiskMgrException e) {
      e.printStackTrace();
    } catch (HFBufMgrException e) {
      e.printStackTrace();
    } catch (FieldNumberOutOfBoundException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }


  public void printIDs() {
    System.out.print("[");
    try {
      for (int i = 1; i <= fieldCount - 1; i++) {
        System.out.print(
            "(" + this.getEIDField(i).getPageNo().pid + "," + this.getEIDField(i).getSlotNo()
                + ")");
      }
      System.out.print("Confidence:: " + getDoubleFld(fieldCount));
      System.out.println("]");

    } catch (Exception e) {
      System.out.println("Error printing BP" + e);
    }
  }


  public boolean findEID(EID eid) {
    boolean found = false;
    try {
      EID e = null;
      for (int i = 1; i <= fieldCount - 1; i++) {
        if (eid.equals(getEIDField(i))) {
          found = true;
          break;
        }

      }
    } catch (Exception e) {
      System.out.print(e);

    }
    return found;
  }


}
