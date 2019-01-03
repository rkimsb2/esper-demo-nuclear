package com.cor.cep;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.cor.cep.util.RandomTemperatureEventGenerator;

@Configuration
public class BootConfig {
	
	@Bean
	public RandomTemperatureEventGenerator eventGenerator() {
		return new RandomTemperatureEventGenerator();
	}

}
