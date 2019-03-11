package com.example.usrMngmt.UserMngmtPOC.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.example.usrMngmt.UserMngmtPOC.DTO.UserDTO;
import com.example.usrMngmt.UserMngmtPOC.model.User;
import com.example.usrMngmt.UserMngmtPOC.repository.UserRepo;
import com.example.usrMngmt.UserMngmtPOC.service.UserService;

@RestController
public class UserController {

	@Autowired
	private UserRepo userRepo;
	@Autowired
	private UserService userService;

	@GetMapping("/users")
	public List<User> getUser() {
		return userRepo.findAll();
	}

	@PostMapping("/users/new")
	public String saveUser(@RequestBody UserDTO userdto) {
		String msg = userService.convertToEntity(userdto);
		return msg;
	}

	@PutMapping(value = "/user/update", headers = "Accept=application/json")
	public ResponseEntity<User> updateUserPartial(@RequestBody List<UserDTO> userDto) {
		String role = null;
		
		for (UserDTO usrdto : userDto) {
			if (!userRepo.existsById(usrdto.getUserid())) {
				return new ResponseEntity<User>(HttpStatus.NOT_FOUND);
			}
			
			/*if (role == null) {
				role = userDto.getRole();
				System.out.println("inside role" + userDto.getRole() + "::" + userDto.getUserName());
			} else {
				int roleCompare = role.compareTo(userDto.getRole());
				if (roleCompare != 0) {
					System.out.println("role" + userDto.getRole() + "::" + userDto.getUserName());
					return "Roles should be same for selected users";
				}

			}
*/
		}
		for (UserDTO usrdto : userDto) {
			String msg = userService.convertToEntity(usrdto);
			
		}
		return new ResponseEntity<User>(HttpStatus.OK);

	}

}
