package academy.backend.market_pulse.homework.seminar10;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Duration;

public final class Main {
    private Main() { }

    public static void main(String[] args) throws IOException {
        URI endpoint = URI.create(args.length > 0 ? args[0] : "http://localhost:1080/v1/instruments");
        int workers = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        try (QuoteClient client = new QuoteClient(endpoint);
             Dispatcher dispatcher = new Dispatcher(workers, client::fetch, System.out::println)) {
            System.out.println("watch SBER 3s 30s | remove SBER | exit");
            System.out.println("HTTP: " + endpoint + "; workers: " + workers);
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while ((line = input.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] command = line.trim().split("\\s+");
                if (command.length == 1 && command[0].equals("exit")) {
                    break;
                }
                try {
                    if (command.length == 4 && command[0].equals("watch")) {
                        dispatcher.watch(command[1], duration(command[2]), duration(command[3]));
                        System.out.println("Подписка добавлена: " + command[1]);
                    } else if (command.length == 2 && command[0].equals("remove")) {
                        System.out.println(dispatcher.remove(command[1]) ? "Подписка удалена" : "Подписка не найдена");
                    } else {
                        System.out.println("Команды: watch <тикер> <интервал> <срок>, remove <тикер>, exit");
                    }
                } catch (IllegalArgumentException | ArithmeticException error) {
                    System.out.println("Ошибка команды: " + error.getMessage());
                }
            }
        }
        System.out.println("Диспетчер и воркеры остановлены");
    }

    static Duration duration(String text) {
        if (text.matches("[0-9]+ms")) {
            return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2)));
        }
        if (text.matches("[0-9]+[smh]")) {
            long value = Long.parseLong(text.substring(0, text.length() - 1));
            return switch (text.charAt(text.length() - 1)) {
                case 's' -> Duration.ofSeconds(value);
                case 'm' -> Duration.ofMinutes(value);
                default -> Duration.ofHours(value);
            };
        }
        throw new IllegalArgumentException("Длительность должна выглядеть как 500ms, 3s, 2m или 1h");
    }
}
