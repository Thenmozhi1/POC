package com.example.usrMngmt.UserMngmtPOC.DTO;

import java.util.ArrayList;
import java.util.List;
import com.example.usrMngmt.UserMngmtPOC.model.User;
import com.example.usrMngmt.UserMngmtPOC.model.WorkRequest;

public class WorkFlowDTO {
	
	private int workFlowId;
	private String WorkFlowtypes;
	
	private List<WorkRequestDTO> workRequestdto;
	
	private List<UserDTO> usersdto = new ArrayList<UserDTO>();

	public WorkFlowDTO() {
		
	}


	public WorkFlowDTO(String workFlowtypes) {
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


	public List<WorkRequestDTO> getWorkRequestdto() {
		return workRequestdto;
	}


	public void setWorkRequestdto(List<WorkRequestDTO> workRequestdto) {
		this.workRequestdto = workRequestdto;
	}


	public List<UserDTO> getUsersdto() {
		return usersdto;
	}


	public void setUsersdto(List<UserDTO> usersdto) {
		this.usersdto = usersdto;
	}


	@Override
	public String toString() {
		return "WorkFlowDTO [workFlowId=" + workFlowId + ", WorkFlowtypes=" + WorkFlowtypes + ", workRequestdto="
				+ workRequestdto + ", usersdto=" + usersdto + "]";
	}

	

}
