package com.example.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.tree.TreeNode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiConsumer;

public class DiagnosticsLogParser {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticsLogParser.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        List<String> strings = readFile("/Users/kushagrathapar/Data/microsoft/sandbox/cosmos-java-sdk-testing/src"
            + "/main/resources/rtag-v2-09to17.json");
        logger.info("Size of file is : {}", strings.size());
        //logger.info("strings first line is : {}", strings.get(0));

        List<String> reversedStrings = new ArrayList<>();
        for (int i = strings.size() - 1; i >= 0; i--) {
            reversedStrings.add(strings.get(i));
        }
        logger.info("Size of reversed file is : {}", reversedStrings.size());
        //logger.info("strings last line is :{}", reversedStrings.get(0));
        List<String> strings1 = parseLogs(reversedStrings);
        Map<String, List<String>> stringListMap = parseLogsByPod(reversedStrings);
        File file = new File("/Users/kushagrathapar/Data/microsoft/sandbox/cosmos-java-sdk-testing/src/main/resources/parsed.json");
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write("");
        strings1.forEach(stringLine -> {
            try {
                fileWriter.append(stringLine);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
//        stringListMap.forEach((pod, line) -> {
//            line.forEach(stringLine -> {
//                try {
//                    fileWriter.append(pod).append("-").append(stringLine);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//        });
        fileWriter.close();
//        Map<String, List<String>> stringListMap = parseLogsByPod(reversedStrings);
//        stringListMap.keySet().forEach(key -> {
//            File file1 = new File("/Users/kushagrathapar/Data/microsoft/sandbox/cosmos-java-sdk-testing/src/main/resources/parsed/"
//                + key + ".json");
//            try {
//                file1.createNewFile();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//            FileWriter fileWriter1 = null;
//            try {
//                fileWriter1 = new FileWriter(file1);
//                fileWriter1.write("");
//                FileWriter finalFileWriter = fileWriter1;
//                stringListMap.get(key).forEach(line -> {
//                    try {
//                        finalFileWriter.append(line);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//                fileWriter1.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
    }

    public static List<String> parseLogs(List<String> lines) throws JsonProcessingException {
        List<String> parsedLogs = new ArrayList<>();
        for (String line : lines) {
            parsedLogs.add(parseLog(line));
        }
        return parsedLogs;
    }

    public static Map<String, List<String>> parseLogsByPod(List<String> lines) throws JsonProcessingException {
        Map<String, List<String>> parsedLogs = new HashMap<>();
        for (String line : lines) {
            String[] strings = parseLogByPod(line);
            if (!parsedLogs.containsKey(strings[0])) {
                parsedLogs.put(strings[0], new ArrayList<>());
            }
            parsedLogs.get(strings[0]).add(strings[1]);
        }
        return parsedLogs;
    }

    public static String parseLog(String line) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(line);
        //logger.info("json node is : {}", jsonNode.toPrettyString());
        JsonNode jsonNode1 = objectMapper.readTree(jsonNode.get("result").get("_raw").asText());
        //logger.info("raw is : {}", jsonNode1.get("log").asText());
        return jsonNode1.get("kubernetes").get("pod_name").asText() + "-" + jsonNode1.get("log").asText();
    }

    public static String[] parseLogByPod(String line) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(line);
        //logger.info("json node is : {}", jsonNode.toPrettyString());
        JsonNode jsonNode1 = objectMapper.readTree(jsonNode.get("result").get("_raw").asText());
        //logger.info("raw is : {}", jsonNode1.get("log").asText());
        String[] podAndLog = new String[2];
        podAndLog[0] = jsonNode1.get("kubernetes").get("pod_name").asText();
        podAndLog[1] = jsonNode1.get("log").asText();
        return podAndLog;
    }

    public static List<String> readFile(String path) throws FileNotFoundException {
        List<String> fileContents = new ArrayList<>();
        File file = new File(path);
        Scanner scanner = new Scanner(file);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            fileContents.add(line);
        }
        scanner.close();
        return fileContents;
    }
}
