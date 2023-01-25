package com.example.kaykastreamsanalytics.services;

import com.example.kaykastreamsanalytics.entities.LigneDeCommande;
import com.example.kaykastreamsanalytics.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class PageEventService {

    //deploye a consumer to get data and show it
/*    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input) -> {
            System.out.println("********************");
            System.out.println(input.toString());
            System.out.println("********************");
        };
    }
    //deploye generate data every 1000 ms
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(null,
                Math.random() > 0.5 ? "P1" : "P2",
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                new Random().nextInt(9000));
    }
    //get PageEvent data modify it and return it
    @Bean
    public Function<PageEvent, PageEvent> pageEventFunction() {
        return (input) -> {
            input.setUser_name(input.getUser_name().toUpperCase()+"-MODIFIED");
            return input;
        };
    }

    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> kStreamFunction(){
        return (input) -> {
            return input
                    .filter((k,v)->v.getDuration_event()>1000)
                    .map((k,v)->new KeyValue<>(v.getName_page(),v.getDuration_event()))
                    //.groupByKey(Grouped.keySerde(Serdes.String()))
                    .groupBy((k,v)->k,Grouped.with(Serdes.String(),Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
                    .count(Materialized.as("page-count"))
                    .toStream()
                    .map((k,v)->new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }*/

    /***********************************************************************************************************/


    @Bean
    public Consumer<LigneDeCommande> pageEventConsumer() {
        return (input) -> {
            System.out.println("********************");
            System.out.println(input.toString());
            System.out.println("********************");
        };
    }
    //deploye generate data every 1000 ms
    @Bean
    public Supplier<LigneDeCommande> pageEventSupplier() {
        return () -> new LigneDeCommande(null,
                Math.random() > 0.25 ? "Produit1" : "Produit2",
                Math.random() > 0.5 ? "Client1" : "Client2",
                new Date(),
                new Random().nextInt(200),
                new Random().nextInt(500),
                new Random().nextInt(1000));

    }
    //get PageEvent data modify it and return it
    @Bean
    public Function<LigneDeCommande, LigneDeCommande> pageEventFunction() {
        return (input) -> {
            input.setClient(input.getClient().toUpperCase()+"-MODIFIED");
            return input;
        };
    }

    @Bean
    public Function<KStream<String,LigneDeCommande>,KStream<String,Long>> kStreamFunction(){
        return (input) -> {
            return input
                    .filter((k,v)->v.getPrix()>10)
                    .map((k,v)->new KeyValue<>(v.getProduit(),v.getQuantite()))
                    //.groupByKey(Grouped.keySerde(Serdes.String()))
                    .groupBy((k,v)->k,Grouped.with(Serdes.String(),Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
                    .count(Materialized.as("client-count"))
                    .toStream()
                    .map((k,v)->new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }


}
