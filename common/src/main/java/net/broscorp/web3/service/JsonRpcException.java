package net.broscorp.web3.service;

/**
 * Surfaces a JSON-RPC error response (i.e. a {@code {"error": ...}} body) as
 * an exception, preserving the numeric error code so retry layers can
 * classify transient overload codes (e.g. {@code -32005} "limit exceeded")
 * without scraping free-form messages.
 */
public class JsonRpcException extends RuntimeException {

    private final String method;
    private final int code;

    public JsonRpcException(String method, int code, String message) {
        super(method + " failed: code=" + code + " message=" + message);
        this.method = method;
        this.code = code;
    }

    public String method() {
        return method;
    }

    public int code() {
        return code;
    }
}
