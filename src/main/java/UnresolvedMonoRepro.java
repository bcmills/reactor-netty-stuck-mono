import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

public final class UnresolvedMonoRepro {
  public static void main(String[] args) throws InterruptedException {
    CountDownLatch requestReceived = new CountDownLatch(1);
    DisposableServer server = HttpServer.create()
        .host("127.0.0.1")
        .port(0)
        .handle((request, response) -> {
          requestReceived.countDown();
          return response.sendString(Mono.just("ok"));
        })
        .bindNow();

    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    // A Java throw escaping Netty's event-loop thread would reach this handler.
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      uncaught.set(error);
      System.err.println("Exception in thread \"" + thread.getName() + "\"");
      error.printStackTrace(System.err);
    });
    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      CountDownLatch signalled = new CountDownLatch(1);
      HttpClient.create()
          .get()
          .uri("http://127.0.0.1:" + server.port())
          .responseSingle((response, body) -> body.asString()
              // Run the simulated linkage failure during inbound response processing.
              .map(value -> { throw new NoSuchMethodError("simulated incompatible dependency"); }))
          // Observe completion, error, or cancellation of the resulting subscription.
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

      if (!requestReceived.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The loopback server did not receive the request");
      }
      // This wait observes signals; it does not add a timeout to the client's Mono.
      signalled.await(2, TimeUnit.SECONDS);
      System.out.println("signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null));
      if (!"none".equals(signal.get()) || uncaught.get() != null) {
        throw new AssertionError("Expected no signal or uncaught exception");
      }
    } finally {
      server.disposeNow();
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
