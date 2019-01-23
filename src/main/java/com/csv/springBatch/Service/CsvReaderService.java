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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	public void readFile() throws FileNotFoundException, IOException {
		Long startTime = System.currentTimeMillis();
		System.out.println("startTime ====>>>>" + startTime); 
		List<Map<String, String>> csvInputList = new CopyOnWriteArrayList<>();

		List<CSVRecord> csvRecords;
		// String fileName = "Duptine&e.csv";
		//final String fileName="C:/Users/Thenmozhi/Desktop/IssueIntakePOC/HRX135.csv";
		//String fileName="C:\\Users\\Thenmozhi\\Desktop\\IssueIntakePOC\\HRX135.csv";
		 List list = listOfFiles();
	        int size = list.size();
	        String path = null;
	        for(int i=0;i<size;i++) {
	         
	        	  path=(String) list.get(i);
	        
	        	 System.out.println("path ====>>>>" + path);	            
	        }
	        
		//String fileName = "HRX135.csv";
	        String fileName = path.substring(path.lastIndexOf('\\')+1);
	        System.out.println("fileName ====>>>>" + fileName);	       
		String fileNameWithoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
		fileInfo = getTheFileId(fileNameWithoutExtension);
		System.out.println("ID ====>>>>" + fileInfo.getId());
		Map<String, Integer> headerMap;
		CSVFormat format = CSVFormat.newFormat(',').withHeader();
		try (BufferedReader inputReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
			CSVParser dataCSVParser = new CSVParser(inputReader, format);
			csvRecords = dataCSVParser.getRecords();
			headerMap = dataCSVParser.getHeaderMap();
			System.out.println("header size:::: " + headerMap);
			// inputReader.close();
		}
		Set<AxcisReportColumn> axislist = fileInfo.getAxcisReportColumn();

		Set<IssueTypeMaster> issueTypeMasterList = fileInfo.getIssueTypeMaster();
		Map<String, String> mapColumn = new HashMap<>();

		for (AxcisReportColumn b : axislist) {
			mapColumn.put(b.getColumns(), b.getStandardObjectMappingMaster().getCommonFormatFieldName());
		}
		for (IssueTypeMaster issueTypeMaster : issueTypeMasterList) {
			mapColumn.put(issueTypeMaster.getIssueType(), issueTypeMaster.getIssueType());
		}
		System.out.println("Axcis map " + mapColumn);

		Map<String, String> columnMapping = new HashMap<>();
		for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
			/*
			 * String columnHeaderSplitedFrmInt =
			 * getSubStringBeforeInteger(entry.getKey()); String lastString =
			 * entry.getKey().substring(columnHeaderSplitedFrmInt.length()); //
			 * System.out.println("columnHeaderSplitedFrmInt:: "
			 * +" columnHeaderSplitedFrmInt+" lastString::"+lastString);
			 * columnMapping.put(entry.getKey(),
			 * mapColumn.get(columnHeaderSplitedFrmInt) + lastString); }
			 */

			if (fileInfo.getId() == 3) {
				String regex = getSubStringBeforeFirstInteger(entry.getKey(), mapColumn);
				if (regex != null) {

					columnMapping.put(entry.getKey(), regex);
				}
			} else {
				String columnHeaderSplitedFrmInt = getSubStringBeforeInteger(entry.getKey());
				String lastString = entry.getKey().substring(columnHeaderSplitedFrmInt.length());
				// System.out.println("columnHeaderSplitedFrmInt:: " +"
				// columnHeaderSplitedFrmInt+" lastString::"+lastString);
				columnMapping.put(entry.getKey(), mapColumn.get(columnHeaderSplitedFrmInt) + lastString);
			}
		}

		System.out.println("Column Mapping====>> " + columnMapping);
		for (CSVRecord record : csvRecords) {
			Map<String, String> inputMap = new LinkedHashMap<>();

			for (Map.Entry<String, Integer> header : headerMap.entrySet()) {
				if (null != columnMapping.get(header.getKey())) {
					inputMap.put(columnMapping.get(header.getKey()), record.get(header.getValue()));
				}

			}
			csvInputList.add(inputMap);

			//System.out.println("Input Map :" + inputMap);
			// inputMap.clear();
		}

		writeToCsv(csvInputList);

		Long endTime = System.currentTimeMillis();
		System.out.println("endTime ====>>>>" + endTime);

		Long diff = endTime - startTime;
		System.out.println("Time Taken(in sec):::::: " + (diff * 0.001));

	}

	
	
	  //Listing all the files present in folder
	 private static List listOfFiles() {
        List<String> results = new ArrayList<String>();
//         File[] files = new File("f:\\csv").listFiles();
        
        File[] files = new File("G:\\code\\springBatch\\src\\main\\resources").listFiles();


        for (File file : files) {
            if (file.isFile()) {
             
             //Getting absolute path for the file and adding to list
             String x=file.getAbsolutePath();
             if(x.contains(".csv")) {
                results.add(x);}
            }
        }
//         System.out.println(results);
        return results;
    }

	private static String getSubStringBeforeFirstInteger(String key, Map<String, String> mapColumn) {

		for (Map.Entry<String, String> mapping : mapColumn.entrySet()) {
			final String regex = mapping.getKey();
			final String headerKey = key;

			System.out.println("regex: " + regex + "  headerKey:: " + headerKey);

			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(headerKey);
			if (matcher.find()) {
				/*System.out.println("Entire match: " + matcher.group());
				System.out.println("GROUP 0 ::" + matcher.group(0));
				System.out.println("GROUP 1 ::" + matcher.group(1));*/
				final String keyFormat = String.format("%s$1", mapColumn.get(regex));
				// System.out.println("Keyformat====>"+keyFormat);
				String hsahKey = matcher.replaceAll(keyFormat);
				System.out.println("hsahKey =+++++==>>" + hsahKey);
				return hsahKey;
			}

		}
		return null;

	}

	private String getSubStringBeforeFirstInteger(String key) {
		Pattern pattern = Pattern.compile("(.*?)(\\d+)(.*)");
		Matcher matcher = pattern.matcher(key);
		while (matcher.find()) {
			System.out.println("group 1: " + matcher.group(1));

		}
		return matcher.group(1);
	}

	private String getSubStringBeforeInteger(String s) {
		return s.split("[0-9]")[0];
	}

	private void writeToCsv(List<Map<String, String>> csvInputList) throws IOException {

		// write to a csv file
		 File file = new File("G:\\code\\BatchCSV\\src\\main\\resources\\CommonFormate.csv");
		//File file = new File("src/main/resources/CommonFormate.csv");
		// Create a File and append if it already exists.
		Writer writer = new FileWriter(file, true);
		// Copy List of Map Object into CSV format at specified File location.
		csvWriter(csvInputList, writer);
		// Read CSV format from
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
		// writer.close();

	}

	private FileInfo getTheFileId(String fileNameWithoutExtension) {

		System.out.println("FILENAME===>>>" + fileNameWithoutExtension);
		return fileInfoRepository.findByName(fileNameWithoutExtension);

	}
}
