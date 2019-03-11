package com.example.usrMngmt.UserMngmtPOC.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.example.usrMngmt.UserMngmtPOC.model.WorkRequest;

public interface WorkRequestRepo extends JpaRepository<WorkRequest, Integer>{
	
	 @Query("Select u from WorkRequest u where u.workRequestTypes=?1 and u.workFlow.workFlowId=?2")
		WorkRequest findByWorkFlowId(String wrkReqType,int workFlowId);


}
