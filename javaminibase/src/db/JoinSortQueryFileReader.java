package db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class JoinSortQueryFileReader {

  private BufferedReader reader = null;
  private String queryString = null;

  public JoinSortQueryFileReader(String queryFile) {
    InputStream inputStream;

    StringBuilder queryBuilder = new StringBuilder();

    try (Stream<String> stream
        = Files.lines(Paths.get(queryFile), StandardCharsets.UTF_8))
    {
      //Read the content with Stream
      stream.forEach(s -> queryBuilder.append(s));
      queryString = queryBuilder.toString();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public JoinSortQuery getQuery() {
    JoinSortQueryFileParser fileParser = new JoinSortQueryFileParser(queryString);
    JoinSortQuery query = fileParser.parseQuery();
    return query;
  }

}
