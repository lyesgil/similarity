package com.suggestion.similarity.service;

import com.suggestion.similarity.cco.CcoAnalysis;
import com.suggestion.similarity.cco.CcoAnalysis2;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ItemSimilarityService {


	@Autowired
	private SparkContext ctx;


	public void calculateSimilarity(String filepath){
		new CcoAnalysis2().process(ctx,filepath);
	}

}
