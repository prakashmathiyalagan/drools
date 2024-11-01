package com.data;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.builder.Results;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;

public class EligibilityChecker {

    public static void main(String[] args) {
        KieServices kieServices = KieServices.Factory.get();
        KieFileSystem kfs = kieServices.newKieFileSystem();
        //...
        Resource res = kieServices.getResources().newFileSystemResource("src/main/resources/rules.drl");
        kfs.write("src/main/resources/rules.drl", res);
        KieBuilder kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
        Results results = kieBuilder.getResults();
        if (results.hasMessages(Message.Level.ERROR)) {
            System.out.println(results.getMessages());
            throw new IllegalStateException("### errors ###");
        }

        KieContainer kieContainer =
                kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());

        KieBase kieBase = kieContainer.getKieBase();
        KieSession kieSession = kieBase.newKieSession();
        Employee employee1 = new Employee();
        employee1.setEmpId("1");
        employee1.setFirstName("E1FN");
        employee1.setExperience(10);
        employee1.setSalary(10000.00);
        Employee employee2 = new Employee();
        employee2.setEmpId("2");
        employee2.setFirstName("E2FN");
        employee2.setExperience(15);
        employee2.setSalary(9.00);
        kieSession.insert(employee1);
        kieSession.insert(employee2);
        kieSession.fireAllRules();
        System.out.println(employee1);
        System.out.println(employee2);
        kieSession.destroy();

    }

    private static KieFileSystem getKieFileSystem(KieServices kieServices) {
        KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
        kieFileSystem.write(ResourceFactory.newClassPathResource("rules.drl"));
        return kieFileSystem;
    }
}
