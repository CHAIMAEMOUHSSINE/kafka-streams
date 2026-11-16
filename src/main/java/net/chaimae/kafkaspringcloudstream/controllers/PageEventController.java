package net.chaimae.kafkaspringcloudstream.controllers;

import net.chaimae.kafkaspringcloudstream.events.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {
    private StreamBridge streamBridge;
    @Autowired
    private InteractiveQueryService interactiveQueryService;

    public PageEventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }
    @GetMapping("/publish")
    public PageEvent send(String name, String topic){
        PageEvent event = new PageEvent(name, Math.random()>0.5?"U1":"U2", new Date(), 10+new Random().nextInt(1000));
        streamBridge.send(topic, event);
        return event;
    }


    @GetMapping(
            path = "/analytics",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<Map<String, Long>> analytics() {

        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> {

                    Map<String, Long> result = new HashMap<>();

                    // Default values for the graph
                    result.put("P1", 0L);
                    result.put("P2", 0L);

                    try {
                        ReadOnlyWindowStore<String, Long> windowStore =
                                interactiveQueryService.getQueryableStore(
                                        "count-store",
                                        QueryableStoreTypes.windowStore()
                                );

                        Instant now = Instant.now();

                        // Query a slightly wider range
                        Instant from = now.minusSeconds(15);

                        try (KeyValueIterator<Windowed<String>, Long> iterator =
                                     windowStore.fetchAll(from, now)) {

                            while (iterator.hasNext()) {

                                KeyValue<Windowed<String>, Long> entry =
                                        iterator.next();

                                String page = entry.key.key();

                                // Keep the latest window per page
                                if (page.equals("P1") || page.equals("P2")) {
                                    result.put(page, entry.value);
                                }
                            }
                        }

                        System.out.println("Analytics: " + result);

                    } catch (Exception e) {
                        System.err.println(
                                "Analytics store unavailable: " + e.getMessage()
                        );
                    }

                    return result;
                });
    }

}



