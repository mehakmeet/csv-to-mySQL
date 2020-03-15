import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * An application class to read all CSV files from a directory and upload the
 * data to mySQL table for further queries.
 * Format : nanoSeconds, filePath, lengthOfFile, operation, bytesRequested, bytesRead, resTime, position, fromPosition
 */
public class MainApplication {

  private long nanoSeconds;
  private String filePath;
  private long lengthOfFile;
  private String operation;
  private long bytesRequested;
  private long bytesRead;
  private long resTime;
  private long position;
  private long fromPosition;

  //Lists the filenames of all CSV in a directory.
  private List<String> filenames = new LinkedList<String>();

  private void listFilesForFolder(final File folder) {
    for (final File fileEntry : folder.listFiles()) {
      if (fileEntry.isDirectory()) {
        listFilesForFolder(fileEntry);
      } else {
        if (fileEntry.getName().contains(".csv"))
          filenames.add(fileEntry.getName());
      }
    }
  }

  /**
   * Reads the CSV files from a directory.
   */
  protected void ReadCsv() {

    //Directory where all the CSV are present.
    final File directoryName = new File("/Users/mehakmeet.singh/RajeshCollab");
    listFilesForFolder(directoryName);
    System.out.println(filenames);

    for (String csvFile : filenames) {
      try {

        //Directory path + csvFile(csv file names)
        BufferedReader csvReader = new BufferedReader(new FileReader(
            "/Users/mehakmeet.singh/RajeshCollab/" + csvFile));

        String lineText;

        while ((lineText = csvReader.readLine()) != null) {
          String[] data = lineText.split(",");
          nanoSeconds = Long.parseLong(data[0]);
          filePath = data[1];
          lengthOfFile = Long.parseLong(data[2]);
          operation = data[3];
          bytesRequested = Long.parseLong(data[4]);
          bytesRead = Long.parseLong(data[5]);
          resTime = Long.parseLong(data[6]);
          position = Long.parseLong(data[7]);
          fromPosition = Long.parseLong(data[8]);

          //Validate data
          if (!validate())
            continue;

          //Upload a row
          uploadData();

        }

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (SQLException e) {
        System.out.println(e);
      }

    }
  }

  /**
   * Uploads a row of data from CSV file to a SQL table in desired database.
   *
   * @throws SQLException
   */
  private void uploadData() throws SQLException {
    Connection conn = connection();
    try {
      PreparedStatement pstat = conn.prepareStatement("INSERT INTO testTable "
          + "VALUES(?,?,?,?,?,?,?,?,?)");
      pstat.setLong(1, nanoSeconds);
      pstat.setString(2, filePath);
      pstat.setLong(3, lengthOfFile);
      pstat.setString(4, operation);
      pstat.setLong(5, bytesRequested);
      pstat.setLong(6, bytesRead);
      pstat.setLong(7, resTime);
      pstat.setLong(8, position);
      pstat.setLong(9, fromPosition);

      pstat.executeUpdate();

    } catch (SQLException e) {
      System.out.println(e);
    } finally {
      //No leaky connections now.
      conn.close();
    }

  }

  /**
   * Tries to form a connection with a database provided the username and
   * password of it.
   *
   * @return if Successful a Connection else NULL.
   */
  private Connection connection() {
    Properties properties = new Properties();
    String path = "/Users/mehakmeet.singh/RajeshCollab/src/main/java/database"
        + ".properties";
    try {
      FileInputStream fin = new FileInputStream(path);
      properties.load(fin);
      return DriverManager.getConnection(properties.getProperty("DB_URL"),
          properties.getProperty("USER"),
          properties.getProperty("PASS"));
    } catch (SQLException e) {
      System.out.println(e);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Validates data to be uploaded from CSV to mySQL.
   *
   * @return true if validation is successful, false otherwise.
   */
  private boolean validate() {

    //todo What kind of validation ?

    return true;
  }

}

/**
 * Demo class to run the application.
 */
class Demo {

  public static void main(String args[]) {

    MainApplication mainApplication = new MainApplication();

    try {
      mainApplication.ReadCsv();
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
