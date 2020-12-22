package ru.gavr.bdd4hadoop.dsl.operations


import groovy.json.JsonOutput
import groovy.transform.Canonical
import groovy.util.logging.Commons
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Canonical
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Commons
class Expect implements Serializable {

    Map<String, Closure> exceptionAssertions = [:]
    Map<String, Closure> resultAssertions = [:]

    def exception() {
        [
                text: [
                        contains    : { expected -> exceptionAssertions["exception_contains_$expected"] =  { String received ->
                            Assertions.isContains("exception", expected, received) }
                        },
                        notcontains : { expected -> exceptionAssertions["exception_not_contains_$expected"] =  { String received ->
                            Assertions.isNotContains("exception", expected, received) }
                        }
                ]
        ]
    }

    def result() {
        [
                text: [
                        contains    : { expected -> resultAssertions["result_contains_$expected"] =  { def received ->
                            Assertions.isContains("result", expected, JsonOutput.toJson(received)) }
                        },
                        notcontains : { expected -> resultAssertions["result_not_contains_$expected"] =  { def received ->
                            Assertions.isNotContains("result", expected, JsonOutput.toJson(received)) }
                        },
                        equalsTo    : { expected -> resultAssertions["result_equals_$expected"] =  { def received ->
                            Assertions.equalsTo("result", expected, JsonOutput.toJson(received)) }
                        },
                ],
                is_Empty   : { -> resultAssertions["result_is_empty"] = { def received -> Assertions.isEmpty(JsonOutput.toJson(received)) } },
                isNothing  : { -> resultAssertions["result_is_nothing"] = { def received -> Assertions.isNothing(JsonOutput.toJson(received)) } },


        ]
    }
}
