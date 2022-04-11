package basicpattern;

import global.EID;
import heap.FieldNumberOutOfBoundException;
import java.io.IOException;

public class BasicPattern {

  private short fieldCount;

  public EID getEIDField(int fieldNo)
      throws IOException, FieldNumberOutOfBoundException {
    return null;
  }

  public double getDoubleFld(int fieldNo)
      throws IOException, FieldNumberOutOfBoundException {
    return 0.0;
  }

  public short numberOfFlds() {
    return fieldCount;
  }

  public void setHdr(short numberOfFields){

  }

  public BasicPattern setDoubleFld(int fieldNo, double val){
    return null;
  }

  public BasicPattern setEIDField(int fieldNo, EID val){
    return null;
  }
}
