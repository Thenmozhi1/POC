package com.csv.springBatch.repository;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.csv.springBatch.model.FileInfo;

public interface FileInfoRepository extends CrudRepository<FileInfo, Long> {
    @Query("select u from FileInfo u where u.fileName=?1")
	FileInfo findByName(String fileNameWithoutExtension);

}
