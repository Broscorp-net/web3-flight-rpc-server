package net.broscorp.web3.service;

/**
 * Marks a structurally-valid JSON-RPC response whose payload nonetheless
 * fails an invariant we expect every well-formed reply to satisfy
 * (null result with no error, receipts-vs-transactions count mismatch, etc).
 *
 * <p>Typically the upstream node returned a partial/empty body under load —
 * the same blast-radius class as HTTP 429 — so retry layers should treat it
 * as transient.
 */
public class MalformedRpcResponseException extends RuntimeException {

    public MalformedRpcResponseException(String message) {
        super(message);
    }
}
