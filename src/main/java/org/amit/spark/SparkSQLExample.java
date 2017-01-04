package org.amit.spark;

import org.amit.domain.CrimeData;
import org.amit.domain.Employee;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


import static org.apache.spark.sql.functions.col;

/**
 * Author amittank.
 *
 * Example of using SparkSQL in different ways.
 */
public class SparkSQLExample {

    public static void main(String[] args){

        SparkSession sparkSession = SparkSession.builder().appName("SparkSQLExample").master("local[4]").getOrCreate();
        generateDFandPerformSqlOperation(sparkSession);

        inferSchemaOfFileAndPerformOperation(sparkSession);
    }

    /**
     * This method generates DataFrame from a list of objects.
     * generateEmployeeList() method manually creates employee records which we are using here
     * and creating a dataframe and performing the operations on the dataframe.
     * @param sparkSession
     */
    private static void generateDFandPerformSqlOperation(SparkSession sparkSession) {
        List<Employee> employeeList = generateEmployeeList();

        //Create an encoder and dataset.
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        Dataset<Employee> employeeDataset = sparkSession.createDataset(employeeList, employeeEncoder);

        //Print the dataset
        employeeDataset.show();

        //Print schema of the data set.
        employeeDataset.printSchema();
        //Perform SQL like operations and display
        employeeDataset.select("name").show();
        employeeDataset.select(col("name"), col("age")).show();
        employeeDataset.filter(col("age").gt("30")).show();
        employeeDataset.groupBy("age").count().show();
        employeeDataset.groupBy("dept").count().show();

        //Create temporary view and perform select operation
        employeeDataset.createOrReplaceTempView("employee");
        Dataset<Row> employeeRow = sparkSession.sql("Select * from employee");
        employeeRow.show();

    }

    /**
     * This method reads a CSV file, converts it in to javaRDD of a domain object.
     * Convert the RDD in to DataSet and query on top of it.
     * @param sparkSession
     */
    private static void inferSchemaOfFileAndPerformOperation(SparkSession sparkSession){
        //Create RDD
        JavaRDD<CrimeData> crimeDataJavaRDD = sparkSession.read().csv("/Users/amittank/Downloads/Crimes_2012-2015.csv").javaRDD().map(new Function<Row, CrimeData>() {
            @Override
            public CrimeData call(Row row) throws Exception {
                CrimeData data = new CrimeData();
                data.setDate_Rptd(row.getString(0));
                data.setDR_NO(row.getString(1));
                data.setDATE_OCC(row.getString(2));
                data.setTIME_OCC(row.getString(3));
                data.setAREA(row.getString(4));
                data.setAREA_NAME(row.getString(5));
                data.setRD(row.getString(6));
                data.setCrm_Cd(row.getString(7));
                data.setCrmCd_Desc(row.getString(8));
                data.setStatus(row.getString(9));
                data.setStatus_Desc(row.getString(10));
                data.setLOCATION(row.getString(11));
                data.setCross_Street(row.getString(12));
                data.setLocation_1(row.getString(13));
                return data;
            }
        });

        //Create data set.
        Dataset<Row> crimeDataRow = sparkSession.createDataFrame(crimeDataJavaRDD, CrimeData.class);
        crimeDataRow.show(3);
        //Create view and query.
        crimeDataRow.createOrReplaceTempView("CrimeData");
        Dataset<Row> hollywoodCrime = sparkSession.sql("Select * from CrimeData where AREA_NAME=\"Hollywood\"");
        hollywoodCrime.show();
    }

    private static List<Employee> generateEmployeeList() {
        List<Employee> employeeList = new ArrayList<Employee>();

            Employee employee = new Employee();
            employee.setEmpId(1000D);
            employee.setAge(25);
            employee.setDept("Finanace");
            employee.setName("John");

        employeeList.add(employee);

            Employee employeeOne = new Employee();
            employeeOne.setEmpId(1001D);
            employeeOne.setAge(30);
            employeeOne.setDept("Finanace");
            employeeOne.setName("Jerry");

        employeeList.add(employeeOne);

            Employee employeeTwo = new Employee();
            employeeTwo.setEmpId(1002D);
            employeeTwo.setAge(25);
            employeeTwo.setDept("HR");
            employeeTwo.setName("Ziva");

        employeeList.add(employeeTwo);

            Employee employeeThree = new Employee();
            employeeThree.setEmpId(1003D);
            employeeThree.setAge(35);
            employeeThree.setDept("HR");
            employeeThree.setName("Jessica");

        employeeList.add(employeeThree);

            Employee employeeFour = new Employee();
            employeeFour.setEmpId(1004D);
            employeeFour.setAge(40);
            employeeFour.setDept("Product");
            employeeFour.setName("Andy");

        employeeList.add(employeeFour);

            Employee employeeFive = new Employee();
            employeeFive.setEmpId(1005D);
            employeeFive.setAge(25);
            employeeFive.setDept("Product");
            employeeFive.setName("Kate");

        employeeList.add(employeeFive);
       return employeeList;
    }
}
