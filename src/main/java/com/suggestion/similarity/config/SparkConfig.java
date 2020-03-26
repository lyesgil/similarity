package com.suggestion.similarity.config;



import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

	@Value("${spark.app.name}")
	private String appName;

	@Value("${spark.master}")
	private String masterUri;


	@Bean
	public SparkConf conf() {

		return new SparkConf(true)
						.setAppName(appName)
						.setMaster(masterUri);//.set("spark.worker.instances", "2");
	}

	@Bean
	public SparkContext sparkContext() {
		return new SparkContext(conf());
	}


}
