package academy.backend.market_pulse.concurrent;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Учебный пул: фиксированное число потоков и общая FIFO-очередь задач.
 * Закрытие отбрасывает очередь и прерывает текущие задачи, затем ожидает потоки.
 * Задачи должны реагировать на interrupt; close вызывается владельцем, не worker.
 */
public final class SimpleThreadPool implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(SimpleThreadPool.class.getName());
    private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
    private final Thread[] workers;
    private final Consumer<RuntimeException> onFailure;
    private final Object lifecycle = new Object();
    private volatile boolean stopped;

    public SimpleThreadPool(int size, Consumer<RuntimeException> onFailure) {
        if (size <= 0) {
            throw new IllegalArgumentException("Число worker-потоков должно быть положительным");
        }
        this.onFailure = Objects.requireNonNull(onFailure);
        workers = new Thread[size];
        for (int i = 0; i < size; i++) {
            workers[i] = Thread.ofPlatform().name("quote-worker-" + (i + 1)).unstarted(this::work);
        }
        for (Thread worker : workers) {
            worker.start();
        }
    }

    public void submit(Runnable task) {
        Objects.requireNonNull(task);
        // Проверка и добавление атомарны относительно закрытия пула.
        synchronized (lifecycle) {
            if (stopped) {
                throw new IllegalStateException("Пул закрыт");
            }
            tasks.add(task);
        }
    }

    private void work() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
            Runnable task;
            try {
                // Очередь сама усыпляет свободный worker и будит его при поступлении задачи.
                task = tasks.take();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            if (stopped) {
                return;
            }
            try {
                task.run();
            } catch (RuntimeException failure) {
                try {
                    onFailure.accept(failure);
                } catch (RuntimeException reportingFailure) {
                    LOGGER.log(System.Logger.Level.ERROR, "Ошибка обработчика задачи", reportingFailure);
                }
            } finally {
                // Отмена одной подписки не должна отменять следующую задачу этого worker.
                // Закрытие всё равно остановит цикл через volatile stopped.
                Thread.interrupted();
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        for (Thread worker : workers) {
            if (worker == Thread.currentThread()) {
                throw new IllegalStateException("Worker не может ожидать закрытия собственного пула");
            }
        }
        synchronized (lifecycle) {
            stopped = true;
            tasks.clear();
            for (Thread worker : workers) {
                worker.interrupt();
            }
        }
        boolean interrupted = Thread.interrupted();
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
            throw new InterruptedException("Ожидание остановки пула было прервано");
        }
    }
}
