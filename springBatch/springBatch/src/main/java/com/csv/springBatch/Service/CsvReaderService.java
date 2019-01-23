package com.csv.springBatch.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.csv.springBatch.model.AxcisReportColumn;
import com.csv.springBatch.model.FileInfo;
import com.csv.springBatch.model.IssueTypeMaster;
import com.csv.springBatch.repository.FileInfoRepository;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

@Service
public class CsvReaderService {
	@Autowired
	private FileInfoRepository fileInfoRepository;
	private FileInfo fileInfo;
	
	public void readFile() throws FileNotFoundException, IOException{
		List<Map<String, String>> csvInputList = new CopyOnWriteArrayList<>();

		List<CSVRecord> csvRecords;
		String fileName = "Duptine&e.csv";
		String fileNameWithoutExtension=fileName.substring(0, fileName.lastIndexOf("."));	
		fileInfo=getTheFileId(fileNameWithoutExtension);
		System.out.println("ID ====>>>>"+fileInfo);
		Map<String, Integer> headerMap;
		CSVFormat format = CSVFormat.newFormat(',').withHeader();
		try (BufferedReader inputReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8))) {
			CSVParser dataCSVParser = new CSVParser(inputReader, format);	         
            csvRecords = dataCSVParser.getRecords(); 
			headerMap = dataCSVParser.getHeaderMap();
			System.out.println("header size:::: " + headerMap);
		}
		Set<AxcisReportColumn> axislist=fileInfo.getAxcisReportColumn();
		
		Set<IssueTypeMaster> issueTypeMasterList=fileInfo.getIssueTypeMaster();
		Map<String,String> mapColumn=new HashMap<>();
		
		for(AxcisReportColumn b:axislist){				
			mapColumn.put(b.getColumns(),b.getStandardObjectMappingMaster().getCommonFormatFieldName());
		}
		for(IssueTypeMaster issueTypeMaster:issueTypeMasterList ){
			mapColumn.put(issueTypeMaster.getIssueType(), issueTypeMaster.getIssueType());
		}
		System.out.println("Axcis map "+mapColumn);
		 for(CSVRecord record : csvRecords){
			 Map<String, String> inputMap = new LinkedHashMap<>();
             for(Map.Entry<String, Integer> header : headerMap.entrySet()){
            	 if(null!=mapColumn.get(header.getKey())){
                 inputMap.put(mapColumn.get(header.getKey()), record.get(header.getValue()));
            	 }

             }
             csvInputList.add(inputMap);

             System.out.println("Input Map :"+inputMap);
         }
		 
		writeToCsv(csvInputList);
			
}
	private void writeToCsv(List<Map<String, String>> csvInputList) throws IOException {
		 
	     //write  to a csv file
        File file = new File("d:\\CommonFormat.csv");
        // Create a File and append if it already exists.
        Writer writer = new FileWriter(file, true);
        Reader reader = new FileReader(file);
        //Copy List of Map Object into CSV format at specified File location.
        csvWriter(csvInputList, writer);
        //Read CSV format from
        csvInputList.forEach(System.out::println);
		
	}
	public static void csvWriter(List<Map<String, String>> csvInputList, Writer writer) throws IOException {
        CsvSchema schema = null;
        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        if (csvInputList != null && !csvInputList.isEmpty()) {
            for (String col : csvInputList.get(0).keySet()) {
                schemaBuilder.addColumn(col);
            }
            schema = schemaBuilder.build().withLineSeparator("\r").withHeader();
        }
        CsvMapper mapper = new CsvMapper();
        mapper.writer(schema).writeValues(writer).writeAll(csvInputList);
        writer.flush();
    }

	private FileInfo getTheFileId(String fileNameWithoutExtension) {
		
		System.out.println("FILENAME===>>>"+fileNameWithoutExtension);
		return fileInfoRepository.findByName(fileNameWithoutExtension);
		
		
	}
}
