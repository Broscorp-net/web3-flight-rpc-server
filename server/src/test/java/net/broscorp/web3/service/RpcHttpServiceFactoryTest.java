package net.broscorp.web3.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;
import net.broscorp.web3.service.RpcHttpServiceFactory.GzipInterceptor;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;

class RpcHttpServiceFactoryTest {

    private static final String BODY =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x10\"}";

    @Test
    void requestsGzipAndInflatesTheResponse() throws IOException {
        AtomicReference<Request> sent = new AtomicReference<>();

        Response response = new GzipInterceptor().intercept(
            chain(sent, req -> gzipped(req, BODY))
        );

        assertThat(sent.get().header("Accept-Encoding")).isEqualTo("gzip");
        assertThat(response.body().string()).isEqualTo(BODY);
    }

    @Test
    void stripsHeadersThatDescribeTheCompressedPayload() throws IOException {
        // Content-Length/Content-Encoding describe the gzip bytes; leaving them
        // on an inflated body would misreport it to anything reading headers.
        Response response = new GzipInterceptor().intercept(
            chain(new AtomicReference<>(), req -> gzipped(req, BODY))
        );

        assertThat(response.header("Content-Encoding")).isNull();
        assertThat(response.header("Content-Length")).isNull();
    }

    @Test
    void passesUncompressedResponseThrough() throws IOException {
        // A node that ignores Accept-Encoding must still work.
        Response response = new GzipInterceptor().intercept(
            chain(new AtomicReference<>(), req -> plain(req, BODY))
        );

        assertThat(response.body().string()).isEqualTo(BODY);
    }

    @Test
    void doesNotOverrideCallerSuppliedAcceptEncoding() throws IOException {
        AtomicReference<Request> sent = new AtomicReference<>();
        Request request = new Request.Builder()
            .url("http://localhost:8545/")
            .header("Accept-Encoding", "identity")
            .build();

        new GzipInterceptor().intercept(
            chain(request, sent, req -> plain(req, BODY))
        );

        assertThat(sent.get().header("Accept-Encoding")).isEqualTo("identity");
    }

    private interface Responder {
        Response respond(Request request) throws IOException;
    }

    private static Interceptor.Chain chain(
        AtomicReference<Request> sent,
        Responder responder
    ) {
        return chain(
            new Request.Builder().url("http://localhost:8545/").build(),
            sent,
            responder
        );
    }

    private static Interceptor.Chain chain(
        Request request,
        AtomicReference<Request> sent,
        Responder responder
    ) {
        Interceptor.Chain chain = mock(Interceptor.Chain.class);
        when(chain.request()).thenReturn(request);
        try {
            when(chain.proceed(any())).thenAnswer(invocation -> {
                Request proceeded = invocation.getArgument(0);
                sent.set(proceeded);
                return responder.respond(proceeded);
            });
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return chain;
    }

    private static Response gzipped(Request request, String body)
        throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(body.getBytes(UTF_8));
        }
        byte[] compressed = out.toByteArray();
        return baseResponse(request)
            .header("Content-Encoding", "gzip")
            .header("Content-Length", String.valueOf(compressed.length))
            .body(ResponseBody.create(MediaType.get("application/json"), compressed))
            .build();
    }

    private static Response plain(Request request, String body) {
        return baseResponse(request)
            .body(ResponseBody.create(MediaType.get("application/json"), body))
            .build();
    }

    private static Response.Builder baseResponse(Request request) {
        return new Response.Builder()
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK");
    }
}
