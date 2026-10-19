import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

public final class NettyEventLoopErrorHook {
  public static void main(String[] args) throws InterruptedException {
    DisposableServer server = HttpServer.create()
        .host("127.0.0.1")
        .port(0)
        .handle((request, response) -> response.sendString(Mono.just("ok")))
        .bindNow();

    AtomicReference<Throwable> handledError = new AtomicReference<>();
    String hookKey = "netty-event-loop-error-hook";
    Schedulers.onHandleError(hookKey, (thread, error) -> {
      handledError.set(error);
      System.err.println("Scheduler error hook received an error on thread \"" + thread.getName() + "\":");
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
            System.err.println("Subscriber received an error through onError:");
            error.printStackTrace(System.err);
            signalled.countDown();
          }, () -> {
            signal.set("onComplete");
            signalled.countDown();
          });

      // Check that map ran before checking whether the subscriber or hook received anything.
      if (!mapCalled.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The failing map callback did not run");
      }

      // Wait to see whether the subscriber receives anything; do not time out the Mono itself.
      signalled.await(2, TimeUnit.SECONDS);

      System.out.println("netty-event-loop-error-hook signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " errorHook=" + (handledError.get() != null));

      if (!"none".equals(signal.get()) || finallySignal.get() != null || handledError.get() != null) {
        throw new AssertionError("Expected subscriber callbacks, doFinally,"
            + " and the scheduler error hook not to run");
      }
    } finally {
      server.disposeNow();
      Schedulers.resetOnHandleError(hookKey);
    }
  }
}
