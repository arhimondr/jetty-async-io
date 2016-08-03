import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class Client
{
    private static final String SERVER_HOST = "127.0.0.1";
    private static final int SERVER_PORT = 8989;
    private static final String SERVLET_PATH = "/async";

    private static final int REQUESTS_COUNT = 1_000_000;
    private static final int THREADS_COUNT = 16;

    public static void main(String[] args)
    {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        AtomicLong requestsLeft = new AtomicLong(REQUESTS_COUNT);

        List<ListenableFuture<Void>> futures = IntStream.range(0, THREADS_COUNT)
                .mapToObj((i) -> new ExecuteRequestTask(SERVER_HOST, SERVER_PORT, SERVLET_PATH, requestsLeft))
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
    }

    private static class ExecuteRequestTask
            implements Callable<Void>
    {
        private final String host;
        private final int port;
        private final String path;
        private final AtomicLong requestsLeft;
        private final Random random = new Random();

        public ExecuteRequestTask(String host, int port, String path, AtomicLong requestsLeft)
        {
            this.host = host;
            this.port = port;
            this.path = path;
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
                try {
                    executeGet(host, port, path);
                }
                catch (IOException | RuntimeException e) {
                    System.out.println("Request failed: " + path + " " + (System.currentTimeMillis() - start) + "ms");
                    e.printStackTrace();
                    continue;
                }
                if (request % 1000 == 0) {
                    System.out.printf("[%d of %d] Finished.\n", request, REQUESTS_COUNT);
                }
            }
            return null;
        }

        private static int executeGet(String host, int port, String path)
                throws IOException
        {
            URL url = new URL(format("http://%s:%d%s", host, port, path));
            URLConnection connection = url.openConnection();
            connection.setConnectTimeout(1_000);
            connection.setReadTimeout(60_000);
            int contentLength;
            try (InputStream in = connection.getInputStream()) {
                contentLength = connection.getHeaderFieldInt("Content-Length", -1);
                checkState(contentLength > 0);
                CountingOutputStream out = new CountingOutputStream(ByteStreams.nullOutputStream());
                ByteStreams.copy(in, out);
                checkState(contentLength == out.getCount());
            }
            return contentLength;
        }
    }
}
