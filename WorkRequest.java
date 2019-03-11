package com.example.usrMngmt.UserMngmtPOC.model;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;

@Entity
public class WorkRequest {
	@Id
	private int workRequestTypeId;
	private String workRequestTypes;
	
	@ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workFlow_id")
	private WorkFlow workFlow;
	
	@OneToOne(mappedBy="workRequest")
	private UserWorkReqTypeLL userWorkReqTypeLL;

	public WorkRequest() {
		
	}

	public WorkRequest(String workRequestTypes) {
		super();
		this.workRequestTypes = workRequestTypes;
	}

	public int getWorkRequestTypeId() {
		return workRequestTypeId;
	}

	public void setWorkRequestTypeId(int workRequestTypeId) {
		this.workRequestTypeId = workRequestTypeId;
	}

	public String getWorkRequestTypes() {
		return workRequestTypes;
	}

	public void setWorkRequestTypes(String workRequestTypes) {
		this.workRequestTypes = workRequestTypes;
	}

	public WorkFlow getWorkFlow() {
		return workFlow;
	}

	public void setWorkFlow(WorkFlow workFlow) {
		this.workFlow = workFlow;
	}
	
	

	public UserWorkReqTypeLL getUserWorkReqTypeLL() {
		return userWorkReqTypeLL;
	}

	public void setUserWorkReqTypeLL(UserWorkReqTypeLL userWorkReqTypeLL) {
		this.userWorkReqTypeLL = userWorkReqTypeLL;
	}

	@Override
	public String toString() {
		return "WorkRequest [workRequestTypeId=" + workRequestTypeId + ", workRequestTypes=" + workRequestTypes
				+ ", userWorkReqTypeLL=" + userWorkReqTypeLL + "]";
	}

	

}
