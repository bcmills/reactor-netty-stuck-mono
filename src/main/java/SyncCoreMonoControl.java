import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public final class SyncCoreMonoControl {
  public static void main(String[] args) {
    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> uncaught.set(error));
    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      NoSuchMethodError failure = new NoSuchMethodError("simulated incompatible dependency");
      Throwable thrown = null;
      try {
        Mono.just("ok")
            .map(value -> { throw failure; })
            .doFinally(finallySignal::set)
            .subscribe(value -> signal.set("value"),
                error -> signal.set("onError"),
                () -> signal.set("onComplete"));
      } catch (NoSuchMethodError error) {
        thrown = error;
      }

      System.out.println("core-sync signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null) + " thrown=" + (thrown != null));
      if (!"none".equals(signal.get()) || finallySignal.get() != null
          || thrown != failure || uncaught.get() != null) {
        throw new AssertionError("Expected the original fatal error to escape subscribe without signals");
      }
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
