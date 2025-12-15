package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 简单的CSV解析器，不依赖外部库
 */
public class ParserCSV {
    /**
     * 解析CSV文件，返回Object[]的迭代器（从文件系统路径加载）
     *
     * @param filepath  文件系统路径
     * @param delimiter 分隔符
     * @return Object[]的迭代器
     * @throws IOException IO异常
     */
    public Iterator<Object[]> load(String filepath, char delimiter) throws IOException {
        // 先尝试从类路径加载
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filepath);
        if (inputStream != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            return parseReader(reader, delimiter);
        }
        
        // 如果类路径中没有找到，则尝试从文件系统加载
        Path path = Paths.get(filepath);
        if (Files.exists(path)) {
            BufferedReader reader = Files.newBufferedReader(path);
            return parseReader(reader, delimiter);
        }
        
        // 都找不到则抛出异常
        throw new IOException("找不到文件: " + filepath);
    }
    
    /**
     * 使用BufferedReader解析CSV内容
     * @param reader BufferedReader
     * @param delimiter 分隔符
     * @return Object[]的迭代器
     * @throws IOException IO异常
     */
    private Iterator<Object[]> parseReader(BufferedReader reader, char delimiter) throws IOException {
        List<Object[]> testCases = new ArrayList<>();
        
        String line;
        
        // 逐行读取文件
        while ((line = reader.readLine()) != null) {
            // 跳过注释行（以#开头）
            if (line.trim().startsWith("#")) {
                continue;
            }
            // 解析一行数据
            Object[] row = parseLine(line, delimiter);
            testCases.add(row);
        }
        
        reader.close();
        return testCases.iterator();
    }
    
    /**
     * 解析单行CSV数据
     * @param line 行数据
     * @param delimiter 分隔符
     * @return 解析后的对象数组
     */
    private Object[] parseLine(String line, char delimiter) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                // 处理引号
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // 双引号表示一个引号字符
                    currentField.append('"');
                    i++; // 跳过下一个引号
                } else {
                    // 切换引号状态
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                // 分隔符且不在引号内，分割字段
                fields.add(currentField.toString());
                currentField.setLength(0); // 清空当前字段
            } else {
                // 普通字符
                currentField.append(c);
            }
        }
        
        // 添加最后一个字段
        fields.add(currentField.toString());
        
        // 转换为Object数组
        return fields.toArray(new Object[0]);
    }
}