package com.example.usrMngmt.UserMngmtPOC.DTO;


import com.example.usrMngmt.UserMngmtPOC.model.User;
import com.example.usrMngmt.UserMngmtPOC.model.WorkRequest;

public class UserWorkReqTypeLLDTO {
	
	private int workRequestTypeId;
	private WorkRequest workRequest;

	private User user;
	private int processorQRBit;
	private int qrPercentage;
	
	public UserWorkReqTypeLLDTO() {
		
	}

	public UserWorkReqTypeLLDTO(int processorQRBit, int qrPercentage) {
		super();
		this.processorQRBit = processorQRBit;
		this.qrPercentage = qrPercentage;
	}

	public int getWorkRequestTypeId() {
		return workRequestTypeId;
	}

	public void setWorkRequestTypeId(int workRequestTypeId) {
		this.workRequestTypeId = workRequestTypeId;
	}

	public WorkRequest getWorkRequest() {
		return workRequest;
	}

	public void setWorkRequest(WorkRequest workRequest) {
		this.workRequest = workRequest;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public int getProcessorQRBit() {
		return processorQRBit;
	}

	public void setProcessorQRBit(int processorQRBit) {
		this.processorQRBit = processorQRBit;
	}

	public int getQrPercentage() {
		return qrPercentage;
	}

	public void setQrPercentage(int qrPercentage) {
		this.qrPercentage = qrPercentage;
	}

	

	


}
