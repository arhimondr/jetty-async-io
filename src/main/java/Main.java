import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Main
{
    private static final String SERVER_HOST = "127.0.0.1";
    private static final int SERVER_PORT = 8989;
    private static final String SERVLET_PATH = "/async";

    private static final int REQUESTS_COUNT = 1_000_000;
    private static final int THREADS_COUNT = 16;

    public static void main(String[] args)
            throws IOException
    {
        List<byte[]> testData = createTestData();
        byte[] expectedResponse = concat(testData);
        JettyServer server = new JettyServer(SERVER_HOST, SERVER_PORT, ImmutableMap.of(SERVLET_PATH, new AsyncServlet(testData)));
        server.start();

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        AtomicLong requestsLeft = new AtomicLong(REQUESTS_COUNT);

        List<ListenableFuture<Void>> futures = IntStream.range(0, THREADS_COUNT)
                .mapToObj((i) -> new ExecuteRequestTask(SERVER_HOST, SERVER_PORT, SERVLET_PATH, expectedResponse, requestsLeft))
                .map(executor::submit)
                .collect(Collectors.toList());

        ListenableFuture<List<Void>> future = Futures.allAsList(futures);

        try {
            future.get();
        }
        catch (InterruptedException e) {
            future.cancel(true);
            // finish execution
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }

        executor.shutdownNow();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e) {
            // finish execution
        }
        server.stop();
    }

    private static byte[] executeGet(String host, int port, String path)
            throws IOException
    {
        URL url = new URL(format("http://%s:%d%s", host, port, path));
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(1_000);
        connection.setReadTimeout(60_000);
        try (InputStream in = connection.getInputStream()) {
            int contentLength = connection.getHeaderFieldInt("Content-Length", -1);
            if (contentLength >= 0) {
                byte[] response = new byte[contentLength];
                ByteStreams.readFully(in, response);
                return response;
            }
            else {
                System.out.println("!!!!! Content-Length is empty");
                return ByteStreams.toByteArray(in);
            }
        }
    }

    private static List<byte[]> createTestData()
    {
        Random random = new Random();
        ImmutableList.Builder<byte[]> result = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            byte[] smallChunk = new byte[15];
            random.nextBytes(smallChunk);
            result.add(smallChunk);
            byte[] bigChunk = new byte[1_000_000];
            random.nextBytes(bigChunk);
            result.add(bigChunk);
        }
        return result.build();
    }

    private static byte[] concat(List<byte[]> buffers)
    {
        return Bytes.concat(buffers.toArray(new byte[][] {}));
    }

    private static class ExecuteRequestTask
            implements Callable<Void>
    {
        private final String host;
        private final int port;
        private final String path;
        private final byte[] expectedResponse;
        private final AtomicLong requestsLeft;
        private final Random random = new Random();

        public ExecuteRequestTask(String host, int port, String path, byte[] expectedResponse, AtomicLong requestsLeft)
        {
            this.host = host;
            this.port = port;
            this.path = path;
            this.expectedResponse = expectedResponse;
            this.requestsLeft = requestsLeft;
        }

        @Override
        public Void call()
        {
            while (!Thread.currentThread().isInterrupted()) {
                long request = requestsLeft.decrementAndGet();
                if (request < 0) {
                    break;
                }
                long start = System.currentTimeMillis();
                String path = this.path + "?" + random.nextInt(Integer.MAX_VALUE);
                byte[] actualResponse;
                try {
                    actualResponse = executeGet(host, port, path);
                }
                catch (IOException | RuntimeException e) {
                    System.out.println("Request failed: " + path + " " + (System.currentTimeMillis() - start) + "ms");
                    e.printStackTrace();
                    continue;
                }
                checkState(Arrays.equals(actualResponse, expectedResponse), "response is different");
                if (request % 1000 == 0) {
                    System.out.printf("[%d of %d] Finished.\n", request, REQUESTS_COUNT);
                }
            }
            return null;
        }
    }

    private static class AsyncServlet
            extends HttpServlet
    {
        private final List<byte[]> dataToSend;

        private AsyncServlet(List<byte[]> dataToSend)
        {
            this.dataToSend = dataToSend;
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException
        {
            int contentLength = dataToSend.stream().mapToInt((array) -> array.length).sum();
            response.setContentLength(contentLength);
            AsyncContext asyncContext = request.startAsync();
            asyncContext.addListener(new AsyncListener()
            {
                @Override
                public void onComplete(AsyncEvent event)
                        throws IOException
                {

                }

                @Override
                public void onTimeout(AsyncEvent event)
                        throws IOException
                {
                }

                @Override
                public void onError(AsyncEvent event)
                        throws IOException
                {
                    new RuntimeException(event.getThrowable()).printStackTrace();
                }

                @Override
                public void onStartAsync(AsyncEvent event)
                        throws IOException
                {

                }
            });
            asyncContext.setTimeout(0);
            ServletOutputStream out = asyncContext.getResponse().getOutputStream();
            HttpOutput jettyOut = (HttpOutput) out;
            AsyncWriteListener writeListener = new AsyncWriteListener(dataToSend, asyncContext, jettyOut);
            try {
                out.setWriteListener(writeListener);
            }
            catch (RuntimeException e) {
                throw new ServletException("Error setting write listener on response: " + out, e);
            }
        }
    }

    public static class AsyncWriteListener
            implements WriteListener
    {
        private final List<byte[]> buffers;
        private final AtomicInteger currentBuffer = new AtomicInteger(0);
        private final AsyncContext async;
        private final HttpOutput out;

        public AsyncWriteListener(List<byte[]> buffers, AsyncContext async, HttpOutput out)
        {
            this.buffers = ImmutableList.copyOf(requireNonNull(buffers, "buffers is null"));
            this.async = requireNonNull(async);
            this.out = requireNonNull(out);
        }

        public void onWritePossible()
                throws IOException
        {
            while (out.isReady()) {
                int bufferIndex = currentBuffer.getAndIncrement();
                if (bufferIndex < buffers.size()) {
                    byte[] buffer = buffers.get(bufferIndex);
                    out.write(buffer, 0, buffer.length);
                }
                else {
                    async.complete();
                    return;
                }
            }
        }

        public void onError(Throwable t)
        {
            System.out.println("Error during async write");
            t.printStackTrace();
            async.complete();
        }
    }

    private static class JettyServer
    {
        private final Server server;

        public JettyServer(String host, int port, Map<String, HttpServlet> servlets)
        {
            QueuedThreadPool threadPool = new QueuedThreadPool(200);
            threadPool.setMinThreads(20);
            threadPool.setName("http-worker");

            server = new Server(threadPool);
            server.addBean(new ErrorHandler());

            HttpConfiguration httpConfiguration = new HttpConfiguration();

            // make test to do not rely on environment cores count
            int acceptors = 1;
            int selectors = 1;
            HttpConnectionFactory http = new HttpConnectionFactory(httpConfiguration);
            ServerConnector httpConnector = new ServerConnector(server, null, null, null, acceptors, selectors, http);
            httpConnector.setName("http");
            httpConnector.setHost(host);
            httpConnector.setPort(port);
            server.addConnector(httpConnector);

            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            context.addFilter(new FilterHolder(new ErrorLogger()), "/*", null);
            for (Map.Entry<String, HttpServlet> servlet : servlets.entrySet()) {
                ServletHolder servletHolder = new ServletHolder(servlet.getValue());
                context.addServlet(servletHolder, servlet.getKey());
            }
            server.setHandler(context);
        }

        public void start()
        {
            try {
                server.start();
                checkState(server.isStarted(), "server is not started");
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        public void stop()
        {
            server.setStopTimeout(0);
            try {
                server.stop();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static class ErrorLogger
            implements Filter
    {
        @Override
        public void init(FilterConfig filterConfig)
                throws ServletException
        {
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                throws IOException, ServletException
        {
            try {
                chain.doFilter(request, response);
            }
            catch (Throwable e) {
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        public void destroy()
        {
        }
    }
}
