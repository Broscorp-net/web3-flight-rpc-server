package net.broscorp.web3.service;

import java.io.IOException;
import java.util.List;
import okhttp3.ConnectionSpec;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.GzipSource;
import okio.Okio;
import org.web3j.protocol.http.HttpService;

/**
 * Builds the {@link HttpService} used for every JSON-RPC call to the node, with
 * gzip response compression requested explicitly.
 *
 * <p>web3j's default client already got gzip implicitly: OkHttp's
 * {@code BridgeInterceptor} adds {@code Accept-Encoding: gzip} to any request
 * that does not carry the header and transparently inflates the response. That
 * made compression invisible in the code and silently dependent on OkHttp
 * internals. Requesting it here makes it explicit and greppable — the wire
 * behaviour is unchanged, but it can no longer be turned off by accident.
 *
 * <p>Note the trade-off this forces: {@code BridgeInterceptor} only inflates
 * responses to requests <em>it</em> added the header to, so setting
 * {@code Accept-Encoding} ourselves disables its transparent inflate and we own
 * decompression from here on — see {@link GzipInterceptor}.
 *
 * <p>Request bodies are deliberately sent uncompressed. A JSON-RPC call is a few
 * hundred bytes, so gzipping it saves nothing measurable, and many providers
 * reject a gzipped request body outright.
 */
public final class RpcHttpServiceFactory {

    private RpcHttpServiceFactory() {}

    /** Creates an {@link HttpService} for {@code url} that requests gzip. */
    public static HttpService create(String url) {
        return new HttpService(url, createClient());
    }

    static OkHttpClient createClient() {
        return new OkHttpClient.Builder()
            // web3j's default client restricts itself to a legacy Infura cipher
            // spec; OkHttp's own default (modern TLS, plus cleartext for a local
            // node over plain http) is both broader and current.
            .connectionSpecs(
                List.of(ConnectionSpec.MODERN_TLS, ConnectionSpec.CLEARTEXT)
            )
            .addInterceptor(new GzipInterceptor())
            .build();
    }

    /**
     * Requests gzip on every call and inflates the response, replacing the
     * transparent handling that setting the header ourselves opts out of.
     */
    static final class GzipInterceptor implements Interceptor {

        private static final String ACCEPT_ENCODING = "Accept-Encoding";
        private static final String CONTENT_ENCODING = "Content-Encoding";
        private static final String CONTENT_LENGTH = "Content-Length";
        private static final String GZIP = "gzip";

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            if (request.header(ACCEPT_ENCODING) == null) {
                request = request
                    .newBuilder()
                    .header(ACCEPT_ENCODING, GZIP)
                    .build();
            }
            return inflate(chain.proceed(request));
        }

        private static Response inflate(Response response) {
            ResponseBody body = response.body();
            if (
                body == null
                    || !GZIP.equalsIgnoreCase(response.header(CONTENT_ENCODING))
            ) {
                return response;
            }
            // Content-Length describes the compressed payload, so it has to go
            // along with Content-Encoding once the body is inflated.
            Headers headers = response
                .headers()
                .newBuilder()
                .removeAll(CONTENT_ENCODING)
                .removeAll(CONTENT_LENGTH)
                .build();
            return response
                .newBuilder()
                .headers(headers)
                .body(
                    ResponseBody.create(
                        body.contentType(),
                        -1L,
                        Okio.buffer(new GzipSource(body.source()))
                    )
                )
                .build();
        }
    }
}
