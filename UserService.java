package com.example.usrMngmt.UserMngmtPOC.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.example.usrMngmt.UserMngmtPOC.DTO.UserDTO;
import com.example.usrMngmt.UserMngmtPOC.DTO.WorkFlowDTO;
import com.example.usrMngmt.UserMngmtPOC.DTO.WorkRequestDTO;
import com.example.usrMngmt.UserMngmtPOC.configuration.UserConfiguration;
import com.example.usrMngmt.UserMngmtPOC.model.User;
import com.example.usrMngmt.UserMngmtPOC.model.UserWorkReqTypeLL;
import com.example.usrMngmt.UserMngmtPOC.model.WorkFlow;
import com.example.usrMngmt.UserMngmtPOC.model.WorkRequest;
import com.example.usrMngmt.UserMngmtPOC.repository.UserRepo;
import com.example.usrMngmt.UserMngmtPOC.repository.UserWorkReqTypeLLRepo;
import com.example.usrMngmt.UserMngmtPOC.repository.WorkFlowRepo;
import com.example.usrMngmt.UserMngmtPOC.repository.WorkRequestRepo;

@Service
public class UserService {
	@Autowired
	private UserRepo userRepo;
	@Autowired
	private WorkFlowRepo workFlowRepo;
	@Autowired
	private WorkRequestRepo workRequestRepo;	
	@Autowired
	private UserConfiguration userConfiguration;
	@Autowired
	private UserWorkReqTypeLLRepo userWorkReqTypeLLRepo;
	

    
	public String convertToEntity(UserDTO userDto) {

		User user = userConfiguration.modelMapper().map(userDto, User.class);
		userRepo.save(user);
	
	
		for (WorkFlowDTO workFlw : userDto.getWorkFlowsdto()) {
			WorkFlow workflwrepo = workFlowRepo.findByWorkFlowType(workFlw.getWorkFlowtypes());
			WorkFlow workFlow = userConfiguration.modelMapper().map(workFlw, WorkFlow.class);
			workFlow.setWorkFlowId(workflwrepo.getWorkFlowId());
			user.getWorkFlows().add(workFlow);
			userRepo.save(user);
			
			
			for(WorkRequestDTO wrkReq:workFlw.getWorkRequestdto()) {
				WorkRequest workRequestTypesRepo = workRequestRepo.findByWorkFlowId(wrkReq.getWorkRequestTypes(),workFlow.getWorkFlowId());
				WorkRequest workRequest = userConfiguration.modelMapper().map(wrkReq, WorkRequest.class);
				workRequest.setWorkRequestTypeId(workRequestTypesRepo.getWorkRequestTypeId());
				workRequest.setWorkFlow(workFlow);
				workFlow.getWorkRequest().add(workRequest);
				workFlowRepo.save(workFlow);
				
				UserWorkReqTypeLL userWorkReqTypeLL=userConfiguration.modelMapper().map(wrkReq.getUserWorkReqTypeLLdto(), UserWorkReqTypeLL.class);
				userWorkReqTypeLL.setWorkRequestTypeId(workRequest.getWorkRequestTypeId());
				userWorkReqTypeLL.setWorkRequest(workRequest);
				userWorkReqTypeLL.setUser(user);
				userWorkReqTypeLLRepo.save(userWorkReqTypeLL);
				
				workRequest.setUserWorkReqTypeLL(userWorkReqTypeLL);
				workRequestRepo.save(workRequest);
			}
			

			
		}

		return "User:" + user.toString()  + " is ADDED successfully !";
	}
	
	
	
	

	



}
