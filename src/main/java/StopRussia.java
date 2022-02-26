import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        Settings settings = objectMapper.convertValue(resources, Settings.class);
        Map<String, Long> counterMap = new HashMap<>();
        List<Runnable> tasks = new ArrayList<>();
        for (String link : settings.getLinks()) {
            Runnable task = () -> {
                while (true) {
                    try {
                        HttpRequest request = HttpRequest.newBuilder().GET().uri(new URI(link)).build();
                        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                        if (counterMap.containsKey(link)) {
                            Long counter = counterMap.get(link);
                            counterMap.put(link, ++counter);
                            if (counter % 100 == 0) {
                                System.out.println("10 requests has been sent to " + link + ", total: " + counter);
                            }
                        } else {
                            counterMap.put(link, 1L);
                        }
                        Thread.sleep(100);
                    } catch (Exception e) {
                        System.out.println("Error " + e + ", link: " + link + ", thread will be sleeping for 1 min");
                        try {
                            Thread.sleep(60000);
                        } catch (InterruptedException ex) {
                            System.out.println("Error " + e + ", link: " + link + ", after sleep");
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
