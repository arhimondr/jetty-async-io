import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Bytes;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.HttpOutput;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class Server
{
    private static final String SERVER_HOST = "0.0.0.0";
    private static final int SERVER_PORT = 8989;
    private static final String SERVLET_PATH = "/async";

    public static void main(String[] args)
            throws IOException
    {
        List<byte[]> testData = createTestData();
        JettyServer server = new JettyServer(SERVER_HOST, SERVER_PORT, ImmutableMap.of(SERVLET_PATH, new AsyncServlet(testData)));
        server.start();
        try {
            TimeUnit.HOURS.sleep(10);
        }
        catch (InterruptedException e) {
            server.stop();
        }
    }

    private static List<byte[]> createTestData()
    {
        Random random = new Random();
        ImmutableList.Builder<byte[]> result = ImmutableList.builder();
        for (int i = 0; i < 5; i++) {
            byte[] smallChunk = new byte[15];
            random.nextBytes(smallChunk);
            result.add(smallChunk);
            byte[] bigChunk = new byte[1_000_00];
            random.nextBytes(bigChunk);
            result.add(bigChunk);
        }
        return result.build();
    }

    private static byte[] concat(List<byte[]> buffers)
    {
        return Bytes.concat(buffers.toArray(new byte[][] {}));
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
                    byte[] bufferCopy = Arrays.copyOf(buffer, buffer.length);
                    out.write(bufferCopy, 0, bufferCopy.length);
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
        private final org.eclipse.jetty.server.Server server;

        public JettyServer(String host, int port, Map<String, HttpServlet> servlets)
        {
            QueuedThreadPool threadPool = new QueuedThreadPool(200);
            threadPool.setMinThreads(20);
            threadPool.setName("http-worker");

            server = new org.eclipse.jetty.server.Server(threadPool);
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
