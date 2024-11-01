package com.data;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.drools.kiesession.rulebase.InternalKnowledgeBase;
import org.drools.kiesession.rulebase.KnowledgeBaseFactory;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.io.ResourceFactory;

import java.util.Arrays;
import java.util.logging.Logger;

public class SparkProcess {
    private static Logger logger = Logger.getLogger(SparkProcess.class.getName());

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        logger.info("Spark session: " + sparkSession);
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
        Dataset<Employee> inputData = sparkSession.createDataset(Arrays.asList(employee1, employee2), Encoders.bean(Employee.class));
        inputData.show(false);
        Resource resource = ResourceFactory.newClassPathResource("rules.drl");
        KnowledgeBuilder knowledgeBuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
        knowledgeBuilder.add(resource, ResourceType.DRL);
        InternalKnowledgeBase internalKnowledgeBase = KnowledgeBaseFactory.newKnowledgeBase();
        internalKnowledgeBase.addPackages(knowledgeBuilder.getKnowledgePackages());
        Dataset<Employee> outputData = inputData.map(new MapFunction<Employee, Employee>() {
            @Override
            public Employee call(Employee employee) throws Exception {
                KieSession session = internalKnowledgeBase.newKieSession();
                session.insert(employee);
                session.fireAllRules();
                return employee;
            }
        }, Encoders.bean(Employee.class));
        outputData.show(false);
    }
}
