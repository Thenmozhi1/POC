package com.example.usrMngmt.UserMngmtPOC.DTO;

import java.util.ArrayList;
import java.util.List;

import com.example.usrMngmt.UserMngmtPOC.model.UserWorkReqTypeLL;
import com.example.usrMngmt.UserMngmtPOC.model.WorkFlow;

public class UserDTO {
	
	private Long userid;
	private String firstName;
	private String lastName;
	private String middleName;
	private String fullName;
	private String adentId;
	private String location;
	private String oraganization;
	//updatable fields
	private String proxy;
	private String supervisor;
		
	private List<WorkFlowDTO> workFlowsdto = new ArrayList<WorkFlowDTO>();
	

	public UserDTO() {
	
	}

	public UserDTO(Long userid, String firstName, String lastName, String middleName, String fullName, String adentId,
			String location, String oraganization, String proxy, String supervisor) {
		super();
		this.userid = userid;
		this.firstName = firstName;
		this.lastName = lastName;
		this.middleName = middleName;
		this.fullName = fullName;
		this.adentId = adentId;
		this.location = location;
		this.oraganization = oraganization;
		this.proxy = proxy;
		this.supervisor = supervisor;
	}

	public Long getUserid() {
		return userid;
	}

	public void setUserid(Long userid) {
		this.userid = userid;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getMiddleName() {
		return middleName;
	}

	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getAdentId() {
		return adentId;
	}

	public void setAdentId(String adentId) {
		this.adentId = adentId;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getOraganization() {
		return oraganization;
	}

	public void setOraganization(String oraganization) {
		this.oraganization = oraganization;
	}

	public String getProxy() {
		return proxy;
	}

	public void setProxy(String proxy) {
		this.proxy = proxy;
	}

	public String getSupervisor() {
		return supervisor;
	}

	public void setSupervisor(String supervisor) {
		this.supervisor = supervisor;
	}

	public List<WorkFlowDTO> getWorkFlowsdto() {
		return workFlowsdto;
	}

	public void setWorkFlowsdto(List<WorkFlowDTO> workFlowsdto) {
		this.workFlowsdto = workFlowsdto;
	}
	
	

	
	@Override
	public String toString() {
		return "UserDTO [userid=" + userid + ", firstName=" + firstName + ", lastName=" + lastName + ", middleName="
				+ middleName + ", fullName=" + fullName + ", adentId=" + adentId + ", location=" + location
				+ ", oraganization=" + oraganization + ", proxy=" + proxy + ", supervisor=" + supervisor
				+ ", workFlowsdto=" + workFlowsdto + "]";
	}

	


}
