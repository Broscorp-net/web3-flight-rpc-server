package net.broscorp.web3.cache;

import org.web3j.protocol.core.methods.response.Log;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryBlockLogsCache implements BlockLogsCache {

    private final Map<BigInteger, List<Log>> map = new ConcurrentHashMap<>();

    @Override
    public void put(BigInteger blockNumber, List<Log> logs) {
        map.put(blockNumber, new ArrayList<>(logs));
    }

    @Override
    public List<Log> get(BigInteger blockNumber) {
        List<Log> logs = map.get(blockNumber);
        return logs == null ? null : new ArrayList<>(logs);
    }

    @Override
    public boolean contains(BigInteger blockNumber) {
        return map.containsKey(blockNumber);
    }

    @Override
    public boolean containsRange(BigInteger startBlock, BigInteger endBlock) {
        for (BigInteger b = startBlock; b.compareTo(endBlock) <= 0; b = b.add(BigInteger.ONE)) {
            if (!map.containsKey(b)) return false;
        }
        return true;
    }
}
