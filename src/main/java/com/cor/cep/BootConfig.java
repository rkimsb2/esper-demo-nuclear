package com.cor.cep;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextStartedEvent;

import com.cor.cep.util.RandomTemperatureEventGenerator;

@Configuration
public class BootConfig implements ApplicationListener<ContextStartedEvent> {
	
	@Value("${event.generate.number:1000}")
	private long noOfTemperatureEvents;
	
	@Autowired
	private RandomTemperatureEventGenerator eventGenerator;
	
	@Bean
	public RandomTemperatureEventGenerator eventGenerator() {
		return new RandomTemperatureEventGenerator();
	}

	@Override
	public void onApplicationEvent(ContextStartedEvent event) {
		eventGenerator.startSendingTemperatureReadings(noOfTemperatureEvents);
	}

}
