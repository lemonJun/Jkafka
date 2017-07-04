package kafka.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

public class ScalaToJava {
    public static final String main = "src/main/java/kafka/";
    public static final String test = "src/test/";

    public static void main(String[] args) throws IOException {
        List<String> resultFileName = Lists.newArrayList();
        List<String> filePaths = ergodic(new File(main), resultFileName);
        for (String p : filePaths) {
            System.out.println(p);
            convertToJava(p, true);
        }
    }

    private static List<String> ergodic(File file, List<String> resultFileName) {
        File[] files = file.listFiles();
        if (files == null)
            return resultFileName;// 判断目录下是不是空的
        for (File f : files) {
            if (f.isDirectory()) {// 判断是否文件夹
                //                resultFileName.add(f.getPath());
                ergodic(f, resultFileName);// 调用自身,查找子目录
            } else
                resultFileName.add(f.getPath());
        }
        return resultFileName;
    }

    public static void convertToJava(String filePath, boolean write) throws IOException {
        List<Character> lastList = Lists.newArrayList('[', '(', '}', '{', ';', '*', '/', ',', '>', '=');
        String p = "(\\w+):\\s?(\\w+)";
        String p1 = "[\\s\\(](Int)\\s";
        String p2 = "public(.+):\\s?(\\w+)\\s?=";
        String r2 = "public $2 $1";
        String p4 = "(\".+\")\\.format\\(";
        String r4 = "String.format($1,";
        String p5 = "for\\((\\w+)\\s?\\<-\\s?(\\d+)\\s+until\\s+(\\d+)\\)";
        String r5 = "for(int $1 = $2; $1 < $3; $1++)";
        String p6 = "\\[(\\S+\\s?\\S+)\\]";
        String r6 = "<$1>";
        StringBuilder content = new StringBuilder();
        boolean comment = false;

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String s;
        while ((s = reader.readLine()) != null) {
            if (s.length() > 0) {
                if (s.contains(" assertEquals")) {
                    s = s.replaceAll("assertEquals", "Assert.assertEquals");
                }
                s = s.replaceAll(p2, r2);
                if (s.contains(" def ")) {
                    s = s.replaceAll("def", "public void ");
                }
                content.append(s.replaceAll(p, "$2 $1").replaceAll(p4, r4).replaceAll(p1, " Integer ").replaceAll(p5, r5).replaceAll(p6, r6));
                if (s.indexOf("/*") != -1) {
                    comment = true;
                }
                if (s.indexOf("*/") != -1) {
                    comment = false;
                }
                if (!comment) {
                    addDelimit(lastList, content, s);
                }
            }
            content.append("\n");
        }
        System.out.println(content.toString());
        if (write) {
            FileWriter writer = new FileWriter(filePath);
            writer.write(content.toString());
            writer.flush();
        }
    }

    private static void addDelimit(List<Character> lastList, StringBuilder content, String s) {
        char last = s.charAt(s.length() - 1);
        boolean inLast = false;
        for (Character c : lastList) {
            if (c == last) {
                inLast = true;
                break;
            }
        }
        if (!inLast) {
            if (!(last == ')' && (s.contains("if") || s.contains("for"))) && !s.contains("@")) {
                content.append(";");
            }
        }
    }
}
