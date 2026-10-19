import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

public final class CoreCreate {
  public static void main(String[] args) {
    AtomicReference<String> signal = new AtomicReference<>("none");
    AtomicReference<SignalType> finallySignal = new AtomicReference<>();
    AtomicReference<Throwable> signalledError = new AtomicReference<>();

    Mono.<String>create(sink -> sink.success("ok"))
        .map(value -> {
          throw new NoSuchMethodError("simulated dependency lacks the required method");
        })
        .doFinally(finallySignal::set)
        .subscribe(value -> signal.set("value"),
            error -> {
              signalledError.set(error);
              signal.set("onError");
              System.err.println("Subscriber received an error through onError:");
              error.printStackTrace(System.err);
            },
            () -> signal.set("onComplete"));

    System.out.println("core-create signal=" + signal.get() + " doFinally="
        + (finallySignal.get() == null ? "none" : finallySignal.get()));

    if (!"onError".equals(signal.get()) || finallySignal.get() != SignalType.ON_ERROR
        || !(signalledError.get() instanceof NoSuchMethodError)) {
      throw new AssertionError("Expected onError to receive NoSuchMethodError and doFinally to run");
    }
  }
}
