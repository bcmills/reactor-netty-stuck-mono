import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public final class CoreSyncSubscribe {
  public static void main(String[] args) {
    AtomicReference<String> signal = new AtomicReference<>("none");
    AtomicReference<SignalType> finallySignal = new AtomicReference<>();
    boolean thrown = false;

    try {
      Mono.just("ok")
          .map(value -> {
            throw new NoSuchMethodError("simulated dependency lacks the required method");
          })
          .doFinally(finallySignal::set)
          .subscribe(value -> signal.set("value"),
              error -> signal.set("onError"),
              () -> signal.set("onComplete"));
    } catch (NoSuchMethodError error) {
      thrown = true;
      System.err.println("Mono.subscribe threw to its caller:");
      error.printStackTrace(System.err);
    }

    System.out.println("core-sync signal=" + signal.get() + " doFinally="
        + (finallySignal.get() == null ? "none" : finallySignal.get())
        + " thrown=" + thrown);

    if (!"none".equals(signal.get()) || finallySignal.get() != null || !thrown) {
      throw new AssertionError("Expected subscribe to throw NoSuchMethodError"
          + " without calling subscriber callbacks or doFinally");
    }
  }
}
