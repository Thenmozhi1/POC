package com.example.usrMngmt.UserMngmtPOC.DTO;



public class WorkRequestDTO {
	
	private int workRequestTypeId;
	private String workRequestTypes;
	
	
	private WorkFlowDTO workFlowdto;
	private UserWorkReqTypeLLDTO userWorkReqTypeLLdto;


	public WorkRequestDTO() {
	}


	public WorkRequestDTO(String workRequestTypes) {
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


	public WorkFlowDTO getWorkFlowdto() {
		return workFlowdto;
	}


	public void setWorkFlowdto(WorkFlowDTO workFlowdto) {
		this.workFlowdto = workFlowdto;
	}


	public UserWorkReqTypeLLDTO getUserWorkReqTypeLLdto() {
		return userWorkReqTypeLLdto;
	}


	public void setUserWorkReqTypeLLdto(UserWorkReqTypeLLDTO userWorkReqTypeLLdto) {
		this.userWorkReqTypeLLdto = userWorkReqTypeLLdto;
	}

}
