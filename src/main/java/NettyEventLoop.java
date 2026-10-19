import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

public final class NettyEventLoop {
  public static void main(String[] args) throws InterruptedException {
    DisposableServer server = HttpServer.create()
        .host("127.0.0.1")
        .port(0)
        .handle((request, response) -> response.sendString(Mono.just("ok")))
        .bindNow();

    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    // If the error escapes Netty's event-loop thread, this handler receives it.
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      uncaught.set(error);
      System.err.println("Default uncaught-exception handler received an error on thread \""
          + thread.getName() + "\":");
      error.printStackTrace(System.err);
    });

    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      CountDownLatch mapCalled = new CountDownLatch(1);
      CountDownLatch signalled = new CountDownLatch(1);

      HttpClient.create()
          .get()
          .uri("http://127.0.0.1:" + server.port())
          .responseSingle((response, body) -> body.asString()
              .map(value -> {
                mapCalled.countDown();
                throw new NoSuchMethodError("simulated dependency lacks the required method");
              }))
          .doFinally(finallySignal::set)
          .subscribe(value -> {
            signal.set("value");
            signalled.countDown();
          }, error -> {
            signal.set("onError");
            signalled.countDown();
          }, () -> {
            signal.set("onComplete");
            signalled.countDown();
          });

      // Check that map ran before checking whether the subscriber received anything.
      if (!mapCalled.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The failing map callback did not run");
      }

      // Wait to see whether the subscriber receives anything; do not time out the Mono itself.
      signalled.await(2, TimeUnit.SECONDS);

      System.out.println("signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null));

      if (!"none".equals(signal.get()) || finallySignal.get() != null || uncaught.get() != null) {
        throw new AssertionError("Expected subscriber callbacks, doFinally,"
            + " and the default uncaught-exception handler not to run");
      }
    } finally {
      server.disposeNow();
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
