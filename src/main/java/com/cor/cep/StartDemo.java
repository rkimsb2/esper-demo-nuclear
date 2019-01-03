package com.cor.cep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.cor.cep.util.RandomTemperatureEventGenerator;

/**
 * Entry point for the Demo. Run this from your IDE, or from the command line using 'mvn exec:java'.
 */
@SpringBootApplication
public class StartDemo {

    /** Logger */
    private static Logger LOG = LoggerFactory.getLogger(StartDemo.class);

    
    /**
     * Main method - start the Demo!
     */
    public static void main(String[] args) throws Exception {

        LOG.debug("Starting...");

        long noOfTemperatureEvents = 1000;

        if (args.length != 1) {
            LOG.debug("No override of number of events detected - defaulting to " + noOfTemperatureEvents + " events.");
        } else {
            noOfTemperatureEvents = Long.valueOf(args[0]);
        }

        // Load spring config
        ConfigurableApplicationContext applicationContext = SpringApplication.run(StartDemo.class, args);
        
        // Start Demo
        RandomTemperatureEventGenerator generator = (RandomTemperatureEventGenerator) applicationContext.getBean("eventGenerator");
        generator.startSendingTemperatureReadings(noOfTemperatureEvents);

    }

}
