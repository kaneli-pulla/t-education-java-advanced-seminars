package academy.backend.market_pulse.homework.seminar10;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public final class SimpleThreadPool implements AutoCloseable {
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    private final Thread[] workers;
    private final Consumer<RuntimeException> errors;
    private final Object lifecycle = new Object();
    private volatile boolean closed;

    public SimpleThreadPool(int size, Consumer<RuntimeException> errors) {
        if (size < 1) {
            throw new IllegalArgumentException("Количество воркеров должно быть положительным");
        }
        this.errors = Objects.requireNonNull(errors);
        workers = new Thread[size];
        for (int i = 0; i < size; i++) {
            workers[i] = new Thread(this::work, "homework-worker-" + (i + 1));
        }
        for (Thread worker : workers) {
            worker.start();
        }
    }

    public void submit(Runnable task) {
        Objects.requireNonNull(task);
        synchronized (lifecycle) {
            if (closed) {
                throw new IllegalStateException("Пул закрыт");
            }
            queue.add(task);
        }
    }

    private void work() {
        while (!closed) {
            try {
                Runnable task = queue.take();
                if (closed) {
                    return;
                }
                try {
                    task.run();
                } catch (RuntimeException failure) {
                    try {
                        errors.accept(failure);
                    } catch (RuntimeException reportingFailure) {
                        System.getLogger(SimpleThreadPool.class.getName())
                                .log(System.Logger.Level.ERROR, "Ошибка обработчика", reportingFailure);
                    }
                } finally {
                    Thread.interrupted();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        for (Thread worker : workers) {
            if (worker == Thread.currentThread()) {
                throw new IllegalStateException("Нельзя закрыть пул из его собственной задачи");
            }
        }
        synchronized (lifecycle) {
            closed = true;
            queue.clear();
            for (Thread worker : workers) {
                worker.interrupt();
            }
        }
        boolean interrupted = false;
        for (Thread worker : workers) {
            while (worker.isAlive()) {
                try {
                    worker.join();
                } catch (InterruptedException error) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
