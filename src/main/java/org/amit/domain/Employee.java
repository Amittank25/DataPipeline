package org.amit.domain;

/**
 * Author amittank
 *
 * Domain object to represent Employee data.
 *
 */
public class Employee {

    private String name;
    private String dept;
    private Double empId;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDept() {
        return dept;
    }

    public void setDept(String dept) {
        this.dept = dept;
    }

    public Double getEmpId() {
        return empId;
    }

    public void setEmpId(Double empId) {
        this.empId = empId;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
