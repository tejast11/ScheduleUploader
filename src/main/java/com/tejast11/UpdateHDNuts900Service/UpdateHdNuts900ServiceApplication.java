package com.tejast11.UpdateHDNuts900Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import jakarta.annotation.PostConstruct;

@SpringBootApplication
@EnableScheduling
public class UpdateHdNuts900ServiceApplication {
	@Autowired
	private UpdateService updateService;
	public static void main(String[] args) {
		SpringApplication.run(UpdateHdNuts900ServiceApplication.class, args);
	}
	@PostConstruct
	public void init(){
		updateService.updateHDNuts900();
	}
}
