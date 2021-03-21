package ru.gavr.bdd4hadoop

import groovy.util.logging.Commons
import org.apache.logging.log4j.ThreadContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.exceptions.ActionResultException
import ru.gavr.bdd4hadoop.exceptions.NoSuchCommandException

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Component
@Commons
class TestsExecutor {

    @Autowired
    TestsStorage testsStorage
    ExecutorService executorService = Executors.newFixedThreadPool(4)

    def executeAll() {
        execute(testsStorage.getAllTest())
    }

    def executeComponents(List<String> components) {
         execute(testsStorage.getTestsByComponents(components))
    }

    def execute(List<CallableTest> tests){
        if(tests.size() == 0){
            ThreadContext.put("testName", "Running tests")
            log.error("Tests size is 0. Nothing to run in \"execute\" method")
            return
        }

        try {
            tests.each { test ->
                def name = test.getTest().getName()
                try {
                    CompletableFuture
                            .runAsync({
                                ThreadContext.put("testName", name)
                                log.info("============= START =============")
                                test.preconditions()}, executorService)
                            .thenRunAsync({
                                ThreadContext.put("testName", name)
                                test.steps()}, executorService)
                            .thenRunAsync({
                                ThreadContext.put("testName", name)
                                test.postconditions()
                                test.getTestResult().setIsPassed(true)}, executorService)
                            .get()
                } catch (Throwable ex) {
                    ThreadContext.put("testName", name)
                    try {
                        throw ex.getCause();
                    } catch (ActionResultException e) {
                        log.error(e.getMessage(), e)
                    } catch (ConnectException|NoSuchCommandException e) {
                        log.error(e)
                        test.getTestResult().setHasCriticalError(true)
                        test.getTestResult().addResultDetails(e as String)
                    } catch(Throwable e) {
                        log.error(e.cause, e)
                        test.getTestResult().setHasCriticalError(true)
                        test.getTestResult().addResultDetails(e.message)
                    }
                } finally {
                    ThreadContext.put("testName", name)
                    test.getTestResult().setIsExecuted(true)
                    log.info("============= FINISH ============\n")
                }
            }

        } catch (Throwable e) {
            ThreadContext.put("testName", "Fatal error")
            log.fatal(e.cause, e)
        } finally {
            ThreadContext.put("testName", "Stopping tests")
            executorService.shutdown()
        }
        return true
    }

}
