package ru.gavr.bdd4hadoop


import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class BeanConfiguration {

    @Bean
    Binding binding(){
        def binding = new Binding([
                "name": "value",
        ])
        binding
    }


}