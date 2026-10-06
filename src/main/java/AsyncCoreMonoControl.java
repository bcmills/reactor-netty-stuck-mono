import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public final class AsyncCoreMonoControl {
  public static void main(String[] args) throws InterruptedException {
    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> uncaught.set(error));
    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      AtomicReference<Throwable> signalledError = new AtomicReference<>();
      AtomicReference<Thread> producer = new AtomicReference<>();
      NoSuchMethodError failure = new NoSuchMethodError("simulated incompatible dependency");
      Mono.<String>create(sink -> {
            // Emit on a plain Java thread with no task/error wrapper.
            Thread thread = new Thread(() -> sink.success("ok"), "core-mono-control");
            producer.set(thread);
            thread.start();
          })
          .map(value -> { throw failure; })
          .doFinally(finallySignal::set)
          .subscribe(value -> signal.set("value"),
              error -> {
                signalledError.set(error);
                signal.set("onError");
              },
              () -> signal.set("onComplete"));

      // Joining observes the completed producer, including its uncaught-exception handler.
      Thread thread = producer.get();
      thread.join(5000);
      if (thread.isAlive()) {
        throw new AssertionError("The control's producer thread did not terminate");
      }
      System.out.println("core-async signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null));
      if (!"onError".equals(signal.get()) || finallySignal.get() != SignalType.ON_ERROR
          || signalledError.get() != failure || uncaught.get() != null) {
        throw new AssertionError("Expected the original fatal error in onError and doFinally");
      }
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
