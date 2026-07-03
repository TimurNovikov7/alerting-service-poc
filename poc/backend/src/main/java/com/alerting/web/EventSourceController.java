package com.alerting.web;

import com.alerting.config.EventSourceProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/event-sources")
@RequiredArgsConstructor
public class EventSourceController {

    private final EventSourceProperties eventSourceProperties;

    @GetMapping
    public List<EventSourceProperties.EventSource> list() {
        return eventSourceProperties.getSources();
    }
}
