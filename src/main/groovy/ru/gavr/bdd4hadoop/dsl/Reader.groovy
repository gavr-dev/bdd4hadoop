package ru.gavr.bdd4hadoop.dsl

import groovy.util.logging.Commons
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.ImportCustomizer
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import ru.gavr.bdd4hadoop.enums.*

@Component
@Commons
class Reader {

    @Autowired
    Binding binding
    @Autowired
    ObjectProvider<TestBuilder> testBuilderObjectProvider
    ClassLoader cl = Thread.currentThread().getContextClassLoader()

    private List<String> testsList = []

    def loadTestsFromDirectory(List<String> pathsList) {
        pathsList.each {
            new File("$it").eachFileRecurse() {
                testsList << it.getAbsolutePath()
            }
        }
        testsList.stream().map({new File(it) }).sorted( {a, b ->
                int fileNumA = 0
                int fileNumB = 0
                try {
                    def nameA = a.getName().split("\\.",2)[0].toUpperCase()
                    def nameB = b.getName().split("\\.",2)[0].toUpperCase()
                    def aIndex = nameA.contains(TestType.SPARK_WITH_HIVE.typeName.toUpperCase()) ? 3 : 1
                    def bIndex = nameB.contains(TestType.SPARK_WITH_HIVE.typeName.toUpperCase()) ? 3 : 1
                    fileNumA = Integer.parseInt(nameA.split("_", )[aIndex])
                    fileNumB = Integer.parseInt(nameB.split("_", )[bIndex])
                } catch (Exception ex) {
                    log.error("Error while sorting file. Correct format is TYPE + '_' + NUMBER + '_' + NAME: " + ex.getMessage())
                }
                return fileNumA > fileNumB ? 1 : (fileNumA < fileNumB) ? -1 : 0
            }).each { newTest it }
    }

    def newTest(File file) {
        log.info("Parsing file " + file.toString())

        CompilerConfiguration configuration = new CompilerConfiguration()
        configuration.setScriptBaseClass(DelegatingScript.class.getName())

        def imports = new ImportCustomizer()
                .addStaticStars(ServiceType.name)
                .addStaticStars(ServerMode.name)
                .addStaticStars(AuthType.name)
                .addStaticStars(LoginType.name)
        configuration.addCompilationCustomizers(imports)

        TestBuilder testBuilder = testBuilderObjectProvider.getObject()
        GroovyShell sh = new GroovyShell(cl, binding, configuration)
        DelegatingScript script = (DelegatingScript) sh.parse(file)
        script.setDelegate(testBuilder)
        try {
            script.run()
        }catch(Throwable t){
            testBuilder.getTestStorage().getUnReadableFiles() << file
            log.error(t.getMessage(), t)
        }
    }
}



