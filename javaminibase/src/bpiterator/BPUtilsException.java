package bpiterator;

import chainexception.ChainException;

public class BPUtilsException extends ChainException {
    public BPUtilsException(String s) {
        super(null, s);
    }

    public BPUtilsException(Exception prev, String s) {
        super(prev, s);
    }
}
