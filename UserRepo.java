package com.example.usrMngmt.UserMngmtPOC.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.example.usrMngmt.UserMngmtPOC.model.User;



public interface UserRepo extends JpaRepository<User, Long>{

}
