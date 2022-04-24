package basicpattern;

import diskmgr.rdf.RdfDB;
import global.*;
import heap.*;
import heap.labelheap.LabelHeapFile;

import java.io.IOException;

import static global.RDFSystemDefs.initRdfDB;


public class BasicPattern implements GlobalConst {

    /**
     * The Maximum size of this BasicPattern
     */
    public static final int maxSize = MINIBASE_PAGESIZE;

    /**
     * To hold data.
     */
    private byte[] data;

    /**
     * The start position of the BasicPattern in data[]
     * Used for reading byte data from certain offsets to
     * read subject, object etc.
     */
    private int basicPatternOffset;

    /**
     * The length of this BasicPattern
     * Dynamic length based on the number of nodes in the
     * basic pattern.
     */
    private int basicPatternLength;

    /**
     * Number of fields in this BasicPattern
     * This includes the number of nodes and the
     * confidence.
     */
    private short fieldCount;

    /**
     * field Array of offsets of the fields in data[]
     * It contains an array of end offsets of nodes and
     * confidence. It also contains the offset of the metadata
     * located at position 0 or fieldCount.
     */
    private short[] fieldOffset;

    /**
     * Class constructor Create a new BasicPattern with length as max_size and BasicPattern offset =
     * 0.
     */
    public BasicPattern() {
        data = new byte[maxSize];
        basicPatternOffset = 0;
        basicPatternLength = maxSize; //TODO: Check if you want to set the length here
    }

    /**
     * Class Constructor create a BasicPattern as per given parameters of bytearray of BP, Offset and
     * length
     *
     * @param aBasicPattern byte array that contains BasicPattern
     * @param offset        offset of  BasicPattern in the byte array
     * @param length        length of the BasicPattern
     */
    public BasicPattern(byte[] aBasicPattern, int offset, int length) {
        data = aBasicPattern;
        basicPatternOffset = offset;
        basicPatternLength = length;
    }

    /**
     * Copy Constructor that copies from other BasicPattern
     *
     * @param fromBasicPattern basicPattern from which new basicPattern to be created
     */
    public BasicPattern(BasicPattern fromBasicPattern) {
        data = fromBasicPattern.getBasicPatternByteArray();
        basicPatternLength = fromBasicPattern.getLength();
        basicPatternOffset = 0;
        fieldCount = fromBasicPattern.numberOfFields();
        fieldOffset = fromBasicPattern.copyFieldOffset();
    }

    /**
     * The Class constructor to Creat a new BasicPattern with length as given size,BasicPattern offset
     * as 0.
     */
    public BasicPattern(int size) {
        data = new byte[size];
        basicPatternOffset = 0;
        basicPatternLength = size;
    }

    /**
     * To Copy a BasicPattern to the current BasicPattern position, fromBasicPattern should have the
     * same length as this BP
     *
     * @param fromBasicPattern BasicPattern that to be copied
     */
    public void basicPatternCopy(BasicPattern fromBasicPattern) {
        byte[] temparray = fromBasicPattern.getBasicPatternByteArray();
        System.arraycopy(temparray, 0, data, basicPatternOffset, basicPatternLength);
    }

    /**
     * To use this instead of the constructer
     *
     * @param aBasicPattern a byte array that contains the BasicPattern
     * @param offset        offset of the BasicPattern in the byte array
     * @param length        length of the BasicPattern
     */

    public void basicPatternInit(byte[] aBasicPattern, int offset, int length) {
        data = aBasicPattern;
        basicPatternOffset = offset;
        basicPatternLength = length;
    }

    /**
     * To Set BasicPattern with the given BasicPattern length and offset
     *
     * @param record a byte array contains the BasicPattern
     * @param offset the offset of the BasicPattern (  0 by default)
     * @param length the length of the BasicPattern
     */
    public void basicPatternSet(byte[] record, int offset, int length) {
        System.arraycopy(record, offset, data, 0, length);
        basicPatternOffset = 0;
        basicPatternLength = length;
    }

    /**
     * To get the length of a BasicPattern, call this method if you did not call setHeader () before
     *
     * @return basicPatternLength of this BasicPattern in bytes
     */
    public int getLength() {
        return basicPatternLength;
    }

    /**
     * To get the length of a BasicPattern, call this method if you did call setHeader () before
     *
     * @return size of this BasicPattern in bytes
     */
    public short size() {
        return ((short) (fieldOffset[fieldCount] - basicPatternOffset));
    }


    /**
     * To get the offset of a BasicPattern
     *
     * @return offset of the BasicPattern in byte array
     */
    public int getOffset() {
        return basicPatternOffset;
    }


    /**
     * To Copy the BasicPattern byte array
     *
     * @return byte[], a byte array contains the BasicPattern, the length of byte array is length of
     * the BasicPattern
     */
    public byte[] getBasicPatternByteArray() {
        byte[] BasicPatternCopy = new byte[basicPatternLength];
        System.arraycopy(data, basicPatternOffset, BasicPatternCopy, 0, basicPatternLength);
        return BasicPatternCopy;
    }

    /**
     * To get the tuple from Basic pattern
     *
     * @return the tuple extracted from Basic Pattern
     */
    public Tuple getTupleFromBasicPattern() {

        Tuple tuple = new Tuple();
        int lengthOfTuple = fieldCount;

        /* Create attrTypes for nodes and confidence.
            The number of attrTypes being created is equal to
            number of nodes * 2 (because every node has to
            store page ID and offset) and one attrType for
            confidence.
         */
        int numberOfNodes = (lengthOfTuple - 1);
        int numOfAttrTypesForNodes = numberOfNodes * 2;

        AttrType[] attrTypes = new AttrType[numOfAttrTypesForNodes + 1];
        int j = 0;

        //[integer attributes for nodes, double for confidence]
        for (j = 0; j < numOfAttrTypesForNodes; j++) {
            attrTypes[j] = new AttrType(AttrType.attrInteger);
        }
        attrTypes[j] = new AttrType(AttrType.attrDouble);
        short[] size = new short[1];

        int spaceForNodes = numOfAttrTypesForNodes * 4; // integer
        //TODO - size is supposed to store string size. Is this 0?
        size[0] = (short) (spaceForNodes + (1 * 8));

        try {
            tuple.setHdr((short) (numOfAttrTypesForNodes + 1), attrTypes, size);
        } catch (InvalidTypeException invalidTypeException) {
            invalidTypeException.printStackTrace();
        } catch (InvalidTupleSizeException invalidTupleSizeException) {
            invalidTupleSizeException.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int i = 0;
        j = 1;

        // Number of nodes is fieldCount - 1
        // 0 index iteration of number of nodes

        for (i = 1; i <= fieldCount - 1; i++) {
            try {
                EID eid = getEIDFieldFromBP(i);
                tuple.setIntFld(j++, eid.getSlotNo());
                tuple.setIntFld(j++, eid.getPageNo().pid);
            } catch (FieldNumberOutOfBoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            tuple.setDoubleFld(j, getDoubleField(fieldCount));
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tuple;
    }

    /**
     * To get the BasicPattern from the given tuple
     *
     * @param tuple tuple from which, BasicPattern needs to extracted
     * @return
     * @throws FieldNumberOutOfBoundException FieldNumber out of bound exception
     * @throws IOException                    IO Exception
     * @throws InvalidTupleSizeException      Invalid Tuple Size Exception
     * @throws InvalidTypeException           Invalid Type Exception
     */
    public BasicPattern getBasicPatternfromTuple(Tuple tuple)
            throws FieldNumberOutOfBoundException, IOException, InvalidTupleSizeException, InvalidTypeException {

        short lengthOfTuple = tuple.noOfFlds();
        AttrType[] attrTypes = new AttrType[(lengthOfTuple - 1) * 2 + 1];

        int j = 0;
        for (j = 0; j < (lengthOfTuple - 1) * 2; j++) {
            attrTypes[j] = new AttrType(AttrType.attrInteger);
        }

        attrTypes[j] = new AttrType(AttrType.attrDouble);
        short[] sizes = new short[1];

        sizes[0] = (short) ((lengthOfTuple - 1) * 2 * 4 + 1 * 8);

        tuple.setHdr(lengthOfTuple, attrTypes, sizes);
        BasicPattern basicPattern = new BasicPattern();

        try {
            basicPattern.setHeader((short) ((lengthOfTuple) / 2 + 1));
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int i = 0;
        j = 0;

        for (i = 0, j = 1; i < (lengthOfTuple / 2); i++) {
            int slotNo = tuple.getIntFld(j++);
            int pageNo = tuple.getIntFld(j++);

            LID lid = new LID(new PageId(pageNo), slotNo);
            EID eid = lid.getEntityID();
            basicPattern.setEIDField(i + 1, eid);

        }
        double minimumConfidence = tuple.getDoubleFld(j);
        basicPattern.setDoubleField(i + 1, minimumConfidence);
        return basicPattern;

    }

    /**
     * To return the data byte array
     *
     * @return data as byte array
     */
    public byte[] returnBasicPatternByteArray() {
        return data;
    }

    /**
     * To Convert pageNo and slotNo field into EID and return it
     *
     *
     * @param fieldNo the field number
     * @return the converted eid
     * @throws IOException                    IO Exception
     * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound
     */
    public EID getEIDFieldFromBP(int fieldNo)
            throws IOException, FieldNumberOutOfBoundException {
        int pageNo, slotNo;
        if ((fieldNo > 0) && (fieldNo <= fieldCount)) {
            pageNo = Convert.getIntValue(fieldOffset[fieldNo - 1], data);
            slotNo = Convert.getIntValue(fieldOffset[fieldNo - 1] + 4, data);
            PageId page = new PageId();
            page.pid = pageNo;
            LID lid = new LID(page, slotNo);
            EID eid = new EID(lid);
            return eid;
        } else {
            throw new FieldNumberOutOfBoundException(null,
                    "BasicPattern:BASICPATTERN_FIELDNO_OUT_OF_BOUND");
        }
    }


    /**
     * To Convert the given field to double
     *
     * @param fieldNo given field number
     * @return
     * @throws IOException                    IO Exception
     * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound Exception
     */
    public double getDoubleField(int fieldNo)
            throws IOException, FieldNumberOutOfBoundException {
        double doubleValue;
        if ((fieldNo <= fieldCount) && (fieldNo > 0)) {
            doubleValue = Convert.getDoubleValue(fieldOffset[fieldNo - 1], data);
            return doubleValue;
        } else {
            throw new FieldNumberOutOfBoundException(null,
                    "BASICPATTERN:BASICPATTERN_FLDNO_OUT_OF_BOUND");
        }
    }

    /**
     * To Set the given field to EID value
     *
     * @param fieldNo given field number
     * @param eid     given EID
     * @throws IOException                    I/O errors
     * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound Error
     */
    public BasicPattern setEIDField(int fieldNo, EID eid)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fieldNo <= fieldCount) && (fieldNo > 0)) {
            Convert.setIntValue(eid.getPageNo().pid, fieldOffset[fieldNo - 1], data);
            Convert.setIntValue(eid.getSlotNo(), fieldOffset[fieldNo - 1] + 4, data);
            return this;
        } else {
            throw new FieldNumberOutOfBoundException(null,
                    "BASICPATTERN :BASIC_PATTERN_FLDNO_OUT_OF_BOUND");
        }
    }

    /**
     * To Set the given field to double value
     *
     * @param fieldNo     given field number
     * @param doubleValue given double value
     * @throws IOException                    IO exception
     * @throws FieldNumberOutOfBoundException BasicPattern field number out of bound
     */
    public BasicPattern setDoubleField(int fieldNo, double doubleValue)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fieldNo > 0) && (fieldNo <= fieldCount)) {
            Convert.setDoubleValue(doubleValue, fieldOffset[fieldNo - 1], data);
            return this;
        } else {
            throw new FieldNumberOutOfBoundException(null,
                    "BASICPATTERN:BASIC PATTERN_FLDNO_OUT_OF_BOUND");
        }

    }

    /**
     * setHeader  To set the header of this BasicPattern.
     *
     * @param numberOfFields number of nodes + 1 (+1 for confidence)
     * @throws IOException               IO Exception
     * @throws InvalidTypeException      Invalid BasicPattern type Error
     * @throws InvalidTupleSizeException BasicPattern size too big Error
     */

    public void setHeader(short numberOfFields) throws InvalidTupleSizeException, IOException {
        if ((numberOfFields + 2) * 2 > maxSize) {
            throw new InvalidTupleSizeException(null, "BP: BASIC PATTERN_SIZE_ERROR");
        }

        fieldCount = numberOfFields;
        Convert.setShortValue(numberOfFields, basicPatternOffset, data);
        fieldOffset = new short[numberOfFields + 1];
        int position = basicPatternOffset + 2;

        fieldOffset[0] = (short) ((numberOfFields + 2) * 2 + basicPatternOffset);

        Convert.setShortValue(fieldOffset[0], position, data);
        position += 2;
        short stringCount = 0;
        short increment;
        int i;

        for (i = 1; i < numberOfFields; i++) {
            increment = 8;
            fieldOffset[i] = (short) (fieldOffset[i - 1] + increment);
            Convert.setShortValue(fieldOffset[i], position, data);
            position += 2;

        }

        increment = 8;

        fieldOffset[numberOfFields] = (short) (fieldOffset[i - 1] + increment);
        Convert.setShortValue(fieldOffset[numberOfFields], position, data);

        basicPatternLength = fieldOffset[numberOfFields] - basicPatternOffset;

        if (basicPatternLength > maxSize) {
            throw new InvalidTupleSizeException(null, "BASICPATTERN: BASIC PATTERN_SIZE_ERROR");
        }
    }


    /**
     * Returns number of fields in this BasicPattern
     *
     * @return the number of fields in this BasicPattern
     */
    public short numberOfFields() {
        return fieldCount;
    }

    /**
     * Makes a copy of the fieldOffset array
     *
     * @return a  new fieldOffset array that is copied from the existing fieldOffset
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
    public void printBasicPattern()
            throws IOException {

        LabelHeapFile entityHeapFile = ((RdfDB) SystemDefs.JavabaseDB).getEntityHeapFile();
        System.out.print("{");
        try {
            for (int i = 1; i <= fieldCount - 1; i++) {
                Label subject = entityHeapFile.getLabel(this.getEIDFieldFromBP(i).returnLID());
                System.out.printf("%20s  ", subject.getLabel());
            }
            System.out.print(getDoubleField(fieldCount));
            System.out.println("}");

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


    public void printDataFromBasicPattern() {
        System.out.print("{");
        try {
            for (int i = 1; i <= fieldCount - 1; i++) {
                System.out.print(
                        "(" + this.getEIDFieldFromBP(i).getPageNo().pid + "," + this.getEIDFieldFromBP(i)
                                .getSlotNo()
                                + ")");
            }
            System.out.print("CONFIDENCE:: " + getDoubleField(fieldCount));
            System.out.println("}");

        } catch (Exception e) {
            System.out.println("Error printing BASICPATTERN" + e);
        }
    }


    /**
     * checks if the given EID is in the basic pattern.
     *
     * @param eid
     * @return true if found else returns false.
     */
    public boolean findEIDInBasicPattern(EID eid) {
        boolean found = false;
        try {
            for (int i = 1; i <= fieldCount - 1; i++) {
                if (eid.equals(getEIDFieldFromBP(i))) {
                    found = true;
                    break;
                }

            }
        } catch (Exception e) {
            System.out.print(e);

        }
        return found;
    }

    public BasicPattern(Tuple tuple) {

        data = new byte[maxSize];
        basicPatternOffset = 0;
        basicPatternLength = maxSize;

        try {
            int no_tuple_fields = tuple.noOfFlds();
            setHeader((short) ((no_tuple_fields - 1) / 2 + 1));
            int j = 1;
            for (int i = 1; i < fieldCount; i++) {
                int slotno = tuple.getIntFld(j++);
                int pageno = tuple.getIntFld(j++);
                PageId page = new PageId(pageno);
                LID lid = new LID(page, slotno);
                EID eid = lid.returnEID();
                setEIDFld(i, eid);
            }
            setDoubleFld(fieldCount, (double) tuple.getDoubleFld(j));
        } catch (Exception e) {
            System.out.println("Error creating basic pattern from tuple" + e);
        }
    }

    public BasicPattern setDoubleFld(int fldNo, double val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setDoubleValue(val, fieldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "BasicPattern:BASIC PATTERN_FLDNO_OUT_OF_BOUND");

    }

    public BasicPattern setEIDFld(int fldNo, EID val)
            throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            Convert.setIntValue(val.getPageNo().pid, fieldOffset[fldNo - 1], data);
            Convert.setIntValue(val.getSlotNo(), fieldOffset[fldNo - 1] + 4, data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "BasicPattern :BASIC_PATTERN_FLDNO_OUT_OF_BOUND");
    }

    public double getDoubleFld(int fldNo)
            throws IOException, FieldNumberOutOfBoundException {
        double val;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            val = Convert.getDoubleValue(fieldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "BasicPattern_FLDNO_OUT_OF_BOUND");
    }

    public EID getEIDFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {

        int pageno, slotno;
        if ((fldNo > 0) && (fldNo <= fieldCount)) {
            pageno = Convert.getIntValue(fieldOffset[fldNo - 1], data);
            slotno = Convert.getIntValue(fieldOffset[fldNo - 1] + 4, data);
            PageId page = new PageId();
            page.pid = pageno;
            LID lid = new LID(page, slotno);
            EID eid = new EID(lid);
            return eid;
        } else {
            throw new FieldNumberOutOfBoundException(null, "BasicPattern_FLDNO_OUT_OF_BOUND");
        }
    }
}
