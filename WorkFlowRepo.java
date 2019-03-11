package com.example.usrMngmt.UserMngmtPOC.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.example.usrMngmt.UserMngmtPOC.model.WorkFlow;

public interface WorkFlowRepo extends JpaRepository<WorkFlow, Integer>{
	@Query("select u from WorkFlow u where u.WorkFlowtypes=?1")
	WorkFlow findByWorkFlowType(String workFlw);

}
