package com.zhuinden.sparkexperiment;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Owner on 2017. 03. 29..
 */
@RequestMapping("api")
@Controller
public class ApiController {
    @Autowired
    WordCount wordCount;

    @RequestMapping("init")
    public ResponseEntity<List<Count>> init() {
    	Date date = new Date();
    	wordCount.init();
    	List<Count> count = wordCount.count();
    	System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println((new Date().getTime() - date.getTime())/ 1000 % 60);
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		count.add(new Count("Time taken in seconds", ((new Date().getTime() - date.getTime())/ 1000 % 60)));
		ResponseEntity<List<Count>> responseEntity = new ResponseEntity<>(count, HttpStatus.OK);
        return responseEntity;
    }
    
    @RequestMapping("wordcount")
    public ResponseEntity<List<Count>> words() {
    	Date date = new Date();
    	List<Count> count = wordCount.count();
    	System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println((new Date().getTime() - date.getTime())/ 1000 % 60);
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		count.add(new Count("Time taken in seconds", ((new Date().getTime() - date.getTime())/ 1000 % 60)));
		ResponseEntity<List<Count>> responseEntity = new ResponseEntity<>(count, HttpStatus.OK);
        return responseEntity;
    }
    
    @RequestMapping("wordcountbyjdbc")
    public ResponseEntity<List<Count>> wordcountbyjdbc() throws Exception {
    	Date date = new Date();
    	List<Count> count = wordCount.countByJdbc();
    	System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println((new Date().getTime() - date.getTime())/ 1000 % 60);
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		count.add(new Count("Time taken in seconds", ((new Date().getTime() - date.getTime())/ 1000 % 60)));
		ResponseEntity<List<Count>> responseEntity = new ResponseEntity<>(count, HttpStatus.OK);
        return responseEntity;
    }
}
