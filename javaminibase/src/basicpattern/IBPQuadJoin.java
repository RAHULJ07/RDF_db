package basicpattern;

import diskmgr.rdf.IStream;

public interface IBPQuadJoin {

  /**
   * gets the next basic pattern
   * @return
   * @throws Exception
   */
  BasicPattern get_next() throws Exception;

  /**
   * closes the join operation
   * @throws Exception
   */
  void close() throws Exception;

  /**
   * gets the stream based on preferred scan operation
   * @return
   * @throws Exception
   */
  IStream getStream() throws Exception;

}
