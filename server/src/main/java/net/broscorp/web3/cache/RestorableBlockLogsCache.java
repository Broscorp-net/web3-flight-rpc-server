package net.broscorp.web3.cache;

public interface RestorableBlockLogsCache extends BlockLogsCache {
    void downloadSnapshot();
    void uploadSnapshot();
}
