package com.example.usrMngmt.UserMngmtPOC.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinTable;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;

import com.fasterxml.jackson.annotation.JsonBackReference;

@Entity
public class User {
	@Id
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
	
	@JsonBackReference
	@ManyToMany(cascade = CascadeType.ALL)
    @JoinTable(name = "USER_WORKFLOW", 
    joinColumns = { @JoinColumn(name = "userid") }, 
    inverseJoinColumns = { @JoinColumn(name = "workFlowId") })
	private List<WorkFlow> workFlows = new ArrayList<WorkFlow>();
	
	

	public User() {
		
	}

	public User(Long userid, String firstName, String lastName, String middleName, String fullName, String adentId,
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

	public List<WorkFlow> getWorkFlows() {
		return workFlows;
	}
	

	public void setWorkFlows(List<WorkFlow> workFlows) {
		this.workFlows = workFlows;
	}
	
	

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adentId == null) ? 0 : adentId.hashCode());
		result = prime * result + ((firstName == null) ? 0 : firstName.hashCode());
		result = prime * result + ((fullName == null) ? 0 : fullName.hashCode());
		result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
		result = prime * result + ((location == null) ? 0 : location.hashCode());
		result = prime * result + ((middleName == null) ? 0 : middleName.hashCode());
		result = prime * result + ((oraganization == null) ? 0 : oraganization.hashCode());
		result = prime * result + ((proxy == null) ? 0 : proxy.hashCode());
		result = prime * result + ((supervisor == null) ? 0 : supervisor.hashCode());
		result = prime * result + ((userid == null) ? 0 : userid.hashCode());
		result = prime * result + ((workFlows == null) ? 0 : workFlows.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		User other = (User) obj;
		if (adentId == null) {
			if (other.adentId != null)
				return false;
		} else if (!adentId.equals(other.adentId))
			return false;
		if (firstName == null) {
			if (other.firstName != null)
				return false;
		} else if (!firstName.equals(other.firstName))
			return false;
		if (fullName == null) {
			if (other.fullName != null)
				return false;
		} else if (!fullName.equals(other.fullName))
			return false;
		if (lastName == null) {
			if (other.lastName != null)
				return false;
		} else if (!lastName.equals(other.lastName))
			return false;
		if (location == null) {
			if (other.location != null)
				return false;
		} else if (!location.equals(other.location))
			return false;
		if (middleName == null) {
			if (other.middleName != null)
				return false;
		} else if (!middleName.equals(other.middleName))
			return false;
		if (oraganization == null) {
			if (other.oraganization != null)
				return false;
		} else if (!oraganization.equals(other.oraganization))
			return false;
		if (proxy == null) {
			if (other.proxy != null)
				return false;
		} else if (!proxy.equals(other.proxy))
			return false;
		if (supervisor == null) {
			if (other.supervisor != null)
				return false;
		} else if (!supervisor.equals(other.supervisor))
			return false;
		if (userid == null) {
			if (other.userid != null)
				return false;
		} else if (!userid.equals(other.userid))
			return false;
		if (workFlows == null) {
			if (other.workFlows != null)
				return false;
		} else if (!workFlows.equals(other.workFlows))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "User [userid=" + userid + ", firstName=" + firstName + ", lastName=" + lastName + ", middleName="
				+ middleName + ", fullName=" + fullName + ", adentId=" + adentId + ", location=" + location
				+ ", oraganization=" + oraganization + ", proxy=" + proxy + ", supervisor=" + supervisor
				+ ", workFlows=" + workFlows + "]";
	}
	
	
	
	
}
