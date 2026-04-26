package net.broscorp.web3.subscription;

/**
 * Thrown when a client requests a block in the backfill range that has not yet
 * been fetched. Mapped to a Flight {@code UNAVAILABLE} status so the client
 * knows to retry later.
 */
public class BackfillPendingException extends RuntimeException {
    public BackfillPendingException(long blockNumber) {
        super("Block " + blockNumber + " is pending backfill; retry later");
    }
}
