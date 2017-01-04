package org.amit.domain;

/**
 * Author amittank
 *
 * Domain object that hold information contained in the csv.
 * Each column in the csv is represented as an object below.
 *
 */
public class CrimeData {

    private String Date_Rptd;
    private String DR_NO;
    private String DATE_OCC;
    private String TIME_OCC;
    private String AREA;
    private String AREA_NAME;
    private String RD;
    private String Crm_Cd;
    private String CrmCd_Desc;
    private String  Status;
    private String Status_Desc;
    private String LOCATION;
    private String Cross_Street;
    private String Location_1;

    public String getDate_Rptd() {
        return Date_Rptd;
    }

    public void setDate_Rptd(String date_Rptd) {
        Date_Rptd = date_Rptd;
    }

    public String getDR_NO() {
        return DR_NO;
    }

    public void setDR_NO(String DR_NO) {
        this.DR_NO = DR_NO;
    }

    public String getDATE_OCC() {
        return DATE_OCC;
    }

    public void setDATE_OCC(String DATE_OCC) {
        this.DATE_OCC = DATE_OCC;
    }

    public String getTIME_OCC() {
        return TIME_OCC;
    }

    public void setTIME_OCC(String TIME_OCC) {
        this.TIME_OCC = TIME_OCC;
    }

    public String getAREA() {
        return AREA;
    }

    public void setAREA(String AREA) {
        this.AREA = AREA;
    }

    public String getAREA_NAME() {
        return AREA_NAME;
    }

    public void setAREA_NAME(String AREA_NAME) {
        this.AREA_NAME = AREA_NAME;
    }

    public String getRD() {
        return RD;
    }

    public void setRD(String RD) {
        this.RD = RD;
    }

    public String getCrm_Cd() {
        return Crm_Cd;
    }

    public void setCrm_Cd(String crm_Cd) {
        Crm_Cd = crm_Cd;
    }

    public String getCrmCd_Desc() {
        return CrmCd_Desc;
    }

    public void setCrmCd_Desc(String crmCd_Desc) {
        CrmCd_Desc = crmCd_Desc;
    }

    public String getStatus() {
        return Status;
    }

    public void setStatus(String status) {
        Status = status;
    }

    public String getStatus_Desc() {
        return Status_Desc;
    }

    public void setStatus_Desc(String status_Desc) {
        Status_Desc = status_Desc;
    }

    public String getLOCATION() {
        return LOCATION;
    }

    public void setLOCATION(String LOCATION) {
        this.LOCATION = LOCATION;
    }

    public String getCross_Street() {
        return Cross_Street;
    }

    public void setCross_Street(String cross_Street) {
        Cross_Street = cross_Street;
    }

    public String getLocation_1() {
        return Location_1;
    }

    public void setLocation_1(String location_1) {
        Location_1 = location_1;
    }
}
