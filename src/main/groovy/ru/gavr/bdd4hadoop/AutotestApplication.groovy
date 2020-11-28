package ru.gavr.bdd4hadoop

import groovy.util.logging.Commons
import org.apache.logging.log4j.ThreadContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

import java.util.stream.Collectors
import java.util.stream.Stream

@SpringBootApplication
@Commons
class AutotestApplication implements ApplicationRunner {

    static void main(String[] args) {
        log.info("STARTING THE APPLICATION")
        SpringApplication.run(AutotestApplication, args)
        log.info("APPLICATION FINISHED")
    }

    @Autowired Reader reader
    @Autowired Binding binding

    @Override
    void run(ApplicationArguments args) {


    }

}
