package diskmgr.rdf;

import basicpattern.BasicPattern;
import heap.Quadruple;

public interface IStream {
  Quadruple getNext() throws Exception;
  void closeStream() throws Exception;
  BasicPattern getNextBasicPatternFromQuadruple();
}
