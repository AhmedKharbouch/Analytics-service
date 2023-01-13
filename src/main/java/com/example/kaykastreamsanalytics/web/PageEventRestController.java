package com.example.kaykastreamsanalytics.web;

import com.example.kaykastreamsanalytics.entities.PageEvent;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;
@AllArgsConstructor
@RestController
public class PageEventRestController {

    private StreamBridge streamBridge;

    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publishEvent(@PathVariable(name = "topic") String topic, @PathVariable(name = "name")  String name) {
        //Page_Event pageEvent = new Page_Event(null, name, Math.random()>0.5?"U1":"U2", new Date(), new Random().nextLong(9000));
        PageEvent pageEvent = new PageEvent(null,name, Math.random()>0.5?"U1":"U2", new Date(), new Random().nextLong(9000));
        streamBridge.send(topic, pageEvent);

    return pageEvent;
    }
}
