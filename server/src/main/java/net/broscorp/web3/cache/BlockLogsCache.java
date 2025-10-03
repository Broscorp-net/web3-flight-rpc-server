package net.broscorp.web3.cache;

import org.web3j.protocol.core.methods.response.Log;

import java.math.BigInteger;
import java.util.List;

public interface BlockLogsCache {

    void put(BigInteger blockNumber, List<Log> logs);
    List<Log> get(BigInteger blockNumber);
    boolean contains(BigInteger blockNumber);
    boolean containsRange(BigInteger startBlock, BigInteger endBlock);
}
