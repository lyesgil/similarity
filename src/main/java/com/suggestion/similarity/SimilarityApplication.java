package com.suggestion.similarity;

import com.suggestion.similarity.service.ItemSimilarityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class SimilarityApplication {
    @Autowired
    private ItemSimilarityService service;

    public static void main(String[] args) {
        SpringApplication.run(SimilarityApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void EventListenerExecute(){
        service.calculateSimilarity("src\\main\\resources\\data\\data.txt");
    }

}
