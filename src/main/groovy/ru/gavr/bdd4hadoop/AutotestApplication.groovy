package ru.gavr.bdd4hadoop

import groovy.util.logging.Commons
import org.apache.logging.log4j.ThreadContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import ru.gavr.bdd4hadoop.dsl.Reader
import ru.gavr.bdd4hadoop.enums.TestType

import java.util.stream.Collectors
import java.util.stream.Stream

@SpringBootApplication
@Commons
class AutotestApplication implements ApplicationRunner {

    static void main(String[] args) {
        ThreadContext.put("testName", "Start")
        log.info("STARTING THE APPLICATION")
        SpringApplication.run(AutotestApplication, args)
        log.info("APPLICATION FINISHED")
    }

    @Autowired TestsStorage testStorage

    @Autowired TestsExecutor testExecutor

    @Autowired Reader reader
    @Autowired Binding binding

    @Override
    void run(ApplicationArguments args) {
        ThreadContext.put("testName", "Run")

        if (!args.containsOption("tests")) {
            throw new IllegalArgumentException("Empty 'tests' parameter");
        }

        if (!args.containsOption("result")) {
            throw new IllegalArgumentException("Empty 'result' parameter");
        }

        List<String> COMPONENTS_TO_BE_TESTED = Stream.of(TestType.values())
                .filter({it.getTypeName() != null })
                .collect({it.getTypeName()})

        def testedComponents = args.containsOption("component")
                ? args.getOptionValues("component")
                : COMPONENTS_TO_BE_TESTED

        log.debug("Start to testing components activated from cli [${testedComponents}];")


        List<String> validTestedComponents = testedComponents.stream()
                .filter({ COMPONENTS_TO_BE_TESTED.contains(it)})
                .collect(Collectors.toList())


        log.debug("${validTestedComponents} components to run;")
        ThreadContext.put("testName", "Files read")

        reader.loadTestsFromDirectory(args.getOptionValues("tests"))


        validTestedComponents.size() == 0
                ? testExecutor.executeAll()
                : testExecutor.executeComponents(validTestedComponents)

        testStorage.printResultsFormatted(args.getOptionValues("result")[0]) // write results to results.md file
    }

}
