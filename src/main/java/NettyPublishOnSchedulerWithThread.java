import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

public final class NettyPublishOnSchedulerWithThread {
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
    CountDownLatch handlerCalled = new CountDownLatch(1);
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      uncaught.set(error);
      System.err.println("Exception in thread \"" + thread.getName() + "\"");
      error.printStackTrace(System.err);
      handlerCalled.countDown();
    });
    Scheduler scheduler = Schedulers.newSingle(task -> new Thread(task, "http-publish-on-handler"));
    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      AtomicReference<NoSuchMethodError> failure = new AtomicReference<>();
      HttpClient.create()
          .get()
          .uri("http://127.0.0.1:" + server.port())
          .responseSingle((response, body) -> body.asString()
              // Move this callback off Netty's event loop before it can throw.
              .publishOn(scheduler)
              .map(value -> {
                NoSuchMethodError error = new NoSuchMethodError("simulated incompatible dependency");
                failure.set(error);
                throw error;
              }))
          .doFinally(finallySignal::set)
          .subscribe(value -> signal.set("value"),
              error -> {
                signal.set("onError");
                System.err.println("Mono subscriber received onError:");
                error.printStackTrace(System.err);
              },
              () -> signal.set("onComplete"));

      if (!requestReceived.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The loopback server did not receive the request");
      }
      if (!handlerCalled.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The default uncaught-exception handler did not receive the fatal error");
      }
      // Wait for the error-handling task to return before observing subscription state.
      CountDownLatch schedulerDrained = new CountDownLatch(1);
      scheduler.schedule(schedulerDrained::countDown);
      if (!schedulerDrained.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The control's scheduler did not finish processing the emission");
      }
      System.out.println("netty-publish-on-handler signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null));
      if (!"none".equals(signal.get()) || finallySignal.get() != null
          || failure.get() == null || uncaught.get() != failure.get()) {
        throw new AssertionError("Expected the original fatal error in the default handler without signals");
      }
    } finally {
      scheduler.dispose();
      server.disposeNow();
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
