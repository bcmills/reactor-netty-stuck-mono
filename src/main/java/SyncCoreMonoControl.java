import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public final class SyncCoreMonoControl {
  public static void main(String[] args) {
    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      uncaught.set(error);
      System.err.println("Exception in thread \"" + thread.getName() + "\"");
      error.printStackTrace(System.err);
    });
    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      AtomicReference<NoSuchMethodError> failure = new AtomicReference<>();
      Throwable thrown = null;
      try {
        Mono.just("ok")
            .map(value -> {
              // Capture the active operator/subscribe call path in the error's stack trace.
              NoSuchMethodError error = new NoSuchMethodError("simulated incompatible dependency");
              failure.set(error);
              throw error;
            })
            .doFinally(finallySignal::set)
            .subscribe(value -> signal.set("value"),
                error -> signal.set("onError"),
                () -> signal.set("onComplete"));
      } catch (NoSuchMethodError error) {
        thrown = error;
        System.err.println("Mono.subscribe threw to its caller:");
        error.printStackTrace(System.err);
      }

      System.out.println("core-sync signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null) + " thrown=" + (thrown != null));
      if (!"none".equals(signal.get()) || finallySignal.get() != null
          || thrown == null || thrown != failure.get() || uncaught.get() != null) {
        throw new AssertionError("Expected the original fatal error to escape subscribe without signals");
      }
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
