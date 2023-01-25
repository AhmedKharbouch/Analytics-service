package com.example.kaykastreamsanalytics.web;

import com.example.kaykastreamsanalytics.entities.LigneDeCommande;
import com.example.kaykastreamsanalytics.entities.PageEvent;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
@AllArgsConstructor
@RestController
@CrossOrigin("*")
public class PageEventRestController {

    private StreamBridge streamBridge;

    private InteractiveQueryService interactiveQueryService;

   /* @GetMapping("/publish/{topic}/{name}")
    public PageEvent publishEvent(@PathVariable(name = "topic") String topic, @PathVariable(name = "name")  String name) {
        //Page_Event pageEvent = new Page_Event(null, name, Math.random()>0.5?"U1":"U2", new Date(), new Random().nextLong(9000));
        PageEvent pageEvent = new PageEvent(null,name, Math.random()>0.5?"U1":"U2", new Date(), new Random().nextLong(9000));
        streamBridge.send(topic, pageEvent);

    return pageEvent;
    }
    @GetMapping(path = "/analytics", produces= MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(3))
                .map(sequence->{
                    Map<String,Long> stringLongMap = new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);
                    KeyValueIterator<Windowed<String>,Long> fetchAll = windowStore.fetchAll(from, now);
                    while (fetchAll.hasNext()){
                        KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        stringLongMap.put(next.key.key(),next.value);
                    }
                    return stringLongMap;
                }).share();
    }

    @GetMapping(path = "/analytics/{page}", produces= MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analytics(@PathVariable(name = "page") String page){
        return Flux.interval(Duration.ofSeconds(3))
                .map(sequence->{
                    Map<String,Long> stringLongMap = new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);
                    //KeyValueIterator<Windowed<String>,Long> fetchAll = windowStore.fetchAll(from, now);
                    WindowStoreIterator<Long> fetchAll = windowStore.fetch(page, from, now);
                    while (fetchAll.hasNext()){
                        //KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        KeyValue<Long, Long> next = fetchAll.next();
                        //stringLongMap.put(next.key.key(),next.value);
                        stringLongMap.put(page,next.value);
                    }
                    return stringLongMap;
                }).share();
    }*/

    @GetMapping("/publish/{client}/{produit}/{quantite}")
    public LigneDeCommande publishEvent(@PathVariable(name = "client") String client, @PathVariable(name = "produit")  String produit,@PathVariable(name = "quantite")  String quantite) {
        //Page_Event pageEvent = new Page_Event(null, name, Math.random()>0.5?"U1":"U2", new Date(), new Random().nextLong(9000));
        LigneDeCommande ligneDeCommande = new LigneDeCommande(
                null,
                produit,
                client,
                new Date(),
                Long.parseLong(quantite) ,
                new Random().nextInt(500),
                new Random().nextInt(1000));
        streamBridge.send(client, ligneDeCommande);

        return ligneDeCommande;
    }
    @GetMapping(path = "/analytics", produces= MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(3))
                .map(sequence->{
                    Map<String,Long> stringLongMap = new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("client-count", QueryableStoreTypes.windowStore());
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);
                    KeyValueIterator<Windowed<String>,Long> fetchAll = windowStore.fetchAll(from, now);
                    while (fetchAll.hasNext()){
                        KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        stringLongMap.put(next.key.key(),next.value);
                    }
                    return stringLongMap;
                }).share();
    }

    @GetMapping(path = "/analytics/{produit}", produces= MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analytics(@PathVariable(name = "produit") String produit){
        return Flux.interval(Duration.ofSeconds(3))
                .map(sequence->{
                    Map<String,Long> stringLongMap = new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);
                    //KeyValueIterator<Windowed<String>,Long> fetchAll = windowStore.fetchAll(from, now);
                    WindowStoreIterator<Long> fetchAll = windowStore.fetch(produit, from, now);
                    while (fetchAll.hasNext()){
                        //KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        KeyValue<Long, Long> next = fetchAll.next();
                        //stringLongMap.put(next.key.key(),next.value);
                        stringLongMap.put(produit,next.value);
                    }
                    return stringLongMap;
                }).share();
    }

}
