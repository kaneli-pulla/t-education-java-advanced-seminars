package academy.backend.market_pulse.homework.seminar10;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;


public final class QuoteClient implements AutoCloseable {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final ObjectMapper json = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final URI endpoint;

    public QuoteClient(URI endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint);
    }

    public BigDecimal fetch(String ticker) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(10)).header("Accept", "application/json").GET().build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("HTTP " + response.statusCode());
        }
        JsonNode instruments = json.readTree(response.body());
        if (instruments == null || !instruments.isArray()) {
            throw new IOException("Ожидался JSON-массив котировок");
        }
        for (JsonNode instrument : instruments) {
            if (ticker.equalsIgnoreCase(instrument.path("ticker").asText())) {
                JsonNode price = instrument.get("price");
                if (price == null || !price.isNumber()) {
                    throw new IOException("Некорректная цена для " + ticker);
                }
                return price.decimalValue();
            }
        }
        throw new IOException("Тикер не найден: " + ticker);
    }

    @Override
    public void close() {
        http.close();
    }
}
