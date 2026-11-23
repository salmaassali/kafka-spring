package org.example.kafkaspringcloudstream.controllers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.example.kafkaspringcloudstream.events.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean; // NOUVEL IMPORT
import org.springframework.kafka.core.KafkaTemplate; // NOUVEL IMPORT (si besoin)
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;

@RestController
public class PageEventController {
    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private InteractiveQueryService interactiveQueryService;
    @GetMapping("/publish")
    public PageEvent publish(String name, String topic){
        PageEvent event = new PageEvent(
                name,
                Math.random()<0.5?"U1":"U2",
                new Date(),10*new Random().nextInt(10000)
        );
        return event;
    }

    @GetMapping(path = "/analytics",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence->{
                    Map<String,Long> stringLongMap=new HashMap<>();

                    // Utilisation de la variable maintenant injectée
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.windowStore());

                    Instant now=Instant.now();
                    Instant from=now.minusMillis(5000);
                    KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now);

                    while (fetchAll.hasNext()){
                        KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        stringLongMap.put(next.key.key(),next.value);
                    }
                    return stringLongMap;
                });
    } // AJOUT DE L'ACCOLADE FERMANTE MANQUANTE POUR LA MÉTHODE
} // AJOUT DE L'ACCOLADE FERMANTE MANQUANTE POUR LA CLASSE