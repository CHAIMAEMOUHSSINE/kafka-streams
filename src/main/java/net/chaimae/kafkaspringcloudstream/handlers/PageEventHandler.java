
package net.chaimae.kafkaspringcloudstream.handlers;

import net.chaimae.kafkaspringcloudstream.events.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    private final Random random = new Random();

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return event -> {
            System.out.println("***************");
            System.out.println(event);
            System.out.println("***************");
        };
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(
                random.nextBoolean() ? "P1" : "P2",
                random.nextBoolean() ? "U1" : "U2",
                new Date(),
                10 + random.nextInt(10000)
        );
    }

    @Bean
    public Function<KStream<String, PageEvent>,
            KStream<String, Long>> kStream() {

        return stream -> stream

                // Keep events with timestamp > 100
                .filter((key, event) ->
                        event.timestamp() > 100)

                // Group events by page
                .map((key, event) ->
                        new KeyValue<>(
                                event.page(),
                                event.timestamp()
                        ))

                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                Serdes.Long()
                        )
                )

                // Five-second windows
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(
                                Duration.ofSeconds(5)
                        )
                )

                // Count events and create state store
                .count(
                        Materialized.as("count-store")
                )

                .toStream()

                // Convert Windowed<String> to String
                .map((windowedKey, count) ->
                        new KeyValue<>(
                                windowedKey.key(),
                                count
                        ));
    }
}
