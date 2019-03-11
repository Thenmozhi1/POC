package com.example.usrMngmt.UserMngmtPOC.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

@Entity
@Table(name = "USER_WR_TYPE_LL")
public class UserWorkReqTypeLL {
	@Id
	private int workRequestTypeId;
	@OneToOne
    @PrimaryKeyJoinColumn(name="workRequestTypeId", referencedColumnName="workRequestTypeId")
	private WorkRequest workRequest;

	@ManyToOne
	@JoinColumn(name = "user_id")
	private User user;
	
	@Column(name = "processor_qr_bit")
	private int processorQRBit;
	@Column(name = "qr_percentage")
	private int qrPercentage;

	public UserWorkReqTypeLL() {

	}

	public UserWorkReqTypeLL(int processorQRBit, int qrPercentage) {
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

	@Override
	public String toString() {
		return "UserWorkReqTypeLL [processorQRBit=" + processorQRBit + ", qrPercentage=" + qrPercentage + "]";
	}
	
	

}
