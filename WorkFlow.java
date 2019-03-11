package com.example.usrMngmt.UserMngmtPOC.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

import com.fasterxml.jackson.annotation.JsonManagedReference;

@Entity
public class WorkFlow {
	@Id	   
	private int workFlowId;
	private String WorkFlowtypes;
	
	@OneToMany(mappedBy="workFlow")
	private List<WorkRequest> workRequest=new ArrayList<WorkRequest>();
	
    @JsonManagedReference
	@ManyToMany(mappedBy= "workFlows")
	private List<User> users = new ArrayList<User>();
	
	

	public WorkFlow() {
		
	}



	public WorkFlow(String workFlowtypes) {
		super();
		WorkFlowtypes = workFlowtypes;
	}



	public int getWorkFlowId() {
		return workFlowId;
	}



	public void setWorkFlowId(int workFlowId) {
		this.workFlowId = workFlowId;
	}



	public String getWorkFlowtypes() {
		return WorkFlowtypes;
	}



	public void setWorkFlowtypes(String workFlowtypes) {
		WorkFlowtypes = workFlowtypes;
	}



	public List<WorkRequest> getWorkRequest() {
		return workRequest;
	}



	public void setWorkRequest(List<WorkRequest> workRequest) {
		this.workRequest = workRequest;
	}



	public List<User> getUsers() {
		return users;
	}



	public void setUsers(List<User> users) {
		this.users = users;
	}

	


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((WorkFlowtypes == null) ? 0 : WorkFlowtypes.hashCode());
		result = prime * result + ((users == null) ? 0 : users.hashCode());
		result = prime * result + workFlowId;
		result = prime * result + ((workRequest == null) ? 0 : workRequest.hashCode());
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
		WorkFlow other = (WorkFlow) obj;
		if (WorkFlowtypes == null) {
			if (other.WorkFlowtypes != null)
				return false;
		} else if (!WorkFlowtypes.equals(other.WorkFlowtypes))
			return false;
		if (users == null) {
			if (other.users != null)
				return false;
		} else if (!users.equals(other.users))
			return false;
		if (workFlowId != other.workFlowId)
			return false;
		if (workRequest == null) {
			if (other.workRequest != null)
				return false;
		} else if (!workRequest.equals(other.workRequest))
			return false;
		return true;
	}



	@Override
	public String toString() {
		return "WorkFlow [workFlowId=" + workFlowId + ", WorkFlowtypes=" + WorkFlowtypes + ", workRequest="
				+ workRequest + "]";
	}
	
	



	
}
