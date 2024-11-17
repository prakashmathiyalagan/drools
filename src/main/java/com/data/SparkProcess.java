package com.data;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
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

        Dataset<Employee> output = inputData.map(new MapFunction<Employee, Employee>() {
            @Override
            public Employee call(Employee employee) throws Exception {
                KieServices kieServices = KieServices.Factory.get();
                KieFileSystem kfs = kieServices.newKieFileSystem();
                Resource res = ResourceFactory.newClassPathResource("rules.drl");
                kfs.write(res);
                KieBuilder kieBuilder = kieServices.newKieBuilder(kfs);
                kieBuilder.buildAll();
                Results results = kieBuilder.getResults();
                if (results.hasMessages(Message.Level.ERROR)) {
                    System.out.println(results.getMessages());
                    throw new IllegalStateException("### errors ###");
                }
                KieContainer kieContainer =
                        kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());

                KieBase kieBase = kieContainer.getKieBase();
                KieSession kieSession = kieBase.newKieSession();
                kieSession.insert(employee);
                kieSession.fireAllRules();

                return employee;
            }
        }, Encoders.bean(Employee.class));
        output.show(false);
    }
}
