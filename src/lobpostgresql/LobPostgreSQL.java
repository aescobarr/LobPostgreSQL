/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package lobpostgresql;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

/**
 *
 * @author a.escobar
 */
public class LobPostgreSQL {
    
    private Connection connection;
    
        
    public HashMap<String,String> getMaunaloaHetznerParams(){
        HashMap<String,String> params = new HashMap<String, String>();
        /* put some values in params */
        return params;
    }
    
    public void openConnection(HashMap<String,String> params){
        try {        
            Class.forName("org.postgresql.Driver");            
            connection = DriverManager.getConnection("jdbc:postgresql://" +
                    params.get("hostname") + 
                    ":" + 
                    params.get("port") +
                    "/" +
                    params.get("dbname"),
                    params.get("username"), 
                    params.get("password")
                    );            
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
                Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
    
    public void closeConnection(){
        if(connection!=null){
            try {
                connection.close();
                connection = null;
            } catch (SQLException ex) {
                Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        HashMap<String,Integer> codis = new HashMap<String, Integer>();
        int totalRecords = 1103;
        int chunkSize = 10;
        boolean keepLooping = true;
        int limit = chunkSize;
        int offset = 0;
        while(keepLooping){
            System.out.println("Limit " + limit);
            System.out.println("Offset " + offset);
            LobPostgreSQL op = new LobPostgreSQL();
            op.openConnection(op.getMaunaloaHetznerParams());
            op.chunkPerformSelect(codis,limit,offset);
            op.closeConnection();
            op = null;
            offset = offset + limit;            
            if (offset > totalRecords) keepLooping = false;
        }                
        //op.createThumbnails();
    }
    
    public void createThumbnails(){
        File folder = new File("C:\\kk\\FotosAfectacio2016");
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {            
          if (listOfFiles[i].isFile()) {
              String originalFile = listOfFiles[i].getAbsolutePath();
              String thumbnailName = removeExtension(listOfFiles[i].getAbsolutePath()) + "_thn.png";
                try {
                    resizePNG(new File(originalFile),new File(thumbnailName),150);
      //            System.out.println("File " + listOfFiles[i].getName());
                } catch (IOException ex) {
                    Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
                }
          }
        }
    }
    
    private String removeExtension(String name) {
        if (name.startsWith(".")) {
            // if there is no extn, do not rmove one...
            if (name.lastIndexOf('.') == name.indexOf('.')) return name;
        }
        // if there is no extention, don't do anything
        if (!name.contains(".")){ 
            return name;
        }
        // Otherwise, remove the last 'extension type thing'
        return name.substring(0, name.lastIndexOf('.'));
    }

    public void chunkPerformSelect(HashMap<String,Integer> codis, int limit, int offset) {
        System.out.println("Select chunk " + limit + " , " + offset);
        PreparedStatement ps;        
        String query = "select af.id, af.dataobservacio, " + 
                    "TO_CHAR(af.dataobservacio,'yyyy'), f.fitxer, f.id, af.codi " +
                    "from afectacio af, sequera s, foto f " +
                    "where af.id = s.id " +
                    "AND TO_CHAR(af.dataobservacio,'yyyy')='2017' " +
                    "AND f.idafectacio = af.id LIMIT %d OFFSET %d";
        query = String.format(query, limit, offset);
        try {
            ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                byte[] imgBytes = rs.getBytes(4);
                FileOutputStream fos;
                try {
                    String codi = rs.getString(6);
                    if(codis.get(codi) == null){
                        codis.put(codi, 1);
                    }else{
                        Integer numcodis = codis.get(codi);
                        numcodis++;
                        codis.put(codi, numcodis);
                        codi = codi + "_" + numcodis;
                    }
                    String path = "F:\\FotosAfectacio2017\\" + codi + ".jpg";
                    fos = new FileOutputStream(path);
                    fos.write(imgBytes);
                    fos.close();
                    fos.flush();
                    fos = null;
                    imgBytes = null;
                    System.out.println("Written " + path );                    
                } catch (IOException ex) {
                    Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
                }                                
            }
            rs.close();        
            ps.close();
            rs = null;
            ps = null;
        } catch (SQLException ex) {
            Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void performSelect() {
        PreparedStatement ps;        
        try {
            ps = connection.prepareStatement("select af.id, af.dataobservacio, " + 
                    "TO_CHAR(af.dataobservacio,'yyyy'), f.fitxer, f.id " +
                    "from afectacio af, sequera s, foto f " +
                    "where af.id = s.id " +
                    "AND TO_CHAR(af.dataobservacio,'yyyy')='2016' " +
                    "AND f.idafectacio = af.id LIMIT 10");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                byte[] imgBytes = rs.getBytes(4);
                FileOutputStream fos;
                try {
                    String path = "C:\\kk\\FotosAfectacio2016\\" + rs.getString(5) + ".jpg";
                    fos = new FileOutputStream(path);
                    fos.write(imgBytes);
                    fos.close();
                    System.out.println("Written " + path );
                } catch (IOException ex) {
                    Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
                }                                
            }
            rs.close();        
        } catch (SQLException ex) {
            Logger.getLogger(LobPostgreSQL.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
    
    public static void resizePNG(File fitxerOriginal, File fitxerOut, int newWidth) throws IOException {

        Image resizedImage = null;

        BufferedImage original = ImageIO.read(fitxerOriginal);
        int iWidth = original.getWidth(null);
        int iHeight = original.getHeight(null);

        if (iWidth > iHeight) {
            resizedImage = original.getScaledInstance(newWidth, (newWidth * iHeight) / iWidth, Image.SCALE_SMOOTH);
        } else {
            resizedImage = original.getScaledInstance((newWidth * iWidth) / iHeight, newWidth, Image.SCALE_SMOOTH);
        }

        // This code ensures that all the pixels in the image are loaded.
        Image temp = new ImageIcon(resizedImage).getImage();

        // Create the buffered image.
        BufferedImage bufferedImage = new BufferedImage(temp.getWidth(null), temp.getHeight(null),
                BufferedImage.TYPE_INT_RGB);

        // Copy image to buffered image.
        Graphics g = bufferedImage.createGraphics();

        // Clear background and paint the image.
        g.setColor(Color.white);
        g.fillRect(0, 0, temp.getWidth(null), temp.getHeight(null));
        g.drawImage(temp, 0, 0, null);
        g.dispose();

        // Soften.
        float softenFactor = 0.05f;
        float[] softenArray = {0, softenFactor, 0, softenFactor, 1 - (softenFactor * 4), softenFactor, 0, softenFactor, 0};
        Kernel kernel = new Kernel(3, 3, softenArray);
        ConvolveOp cOp = new ConvolveOp(kernel, ConvolveOp.EDGE_NO_OP, null);
        bufferedImage = cOp.filter(bufferedImage, null);

        storePNG(bufferedImage, fitxerOut);
        
    }
    
    public static void storePNG(BufferedImage outImage, String path) throws FileNotFoundException, IOException {
        ImageIO.write(outImage, "png", new File(path));
    }
    
    public static void storePNG(BufferedImage outImage, File path) throws FileNotFoundException, IOException {
        ImageIO.write(outImage, "png", path);
    }
    
    public static BufferedImage resizePNG(BufferedImage original, int newWidth) throws IOException {

        Image resizedImage = null;

        int iWidth = original.getWidth(null);
        int iHeight = original.getHeight(null);

        if (iWidth > iHeight) {
            resizedImage = original.getScaledInstance(newWidth, (newWidth * iHeight) / iWidth, Image.SCALE_SMOOTH);
        } else {
            resizedImage = original.getScaledInstance((newWidth * iWidth) / iHeight, newWidth, Image.SCALE_SMOOTH);
        }

        // This code ensures that all the pixels in the image are loaded.
        Image temp = new ImageIcon(resizedImage).getImage();

        // Create the buffered image.
        BufferedImage bufferedImage = new BufferedImage(temp.getWidth(null), temp.getHeight(null),
                BufferedImage.TYPE_INT_RGB);

        // Copy image to buffered image.
        Graphics g = bufferedImage.createGraphics();

        // Clear background and paint the image.
        g.setColor(Color.white);
        g.fillRect(0, 0, temp.getWidth(null), temp.getHeight(null));
        g.drawImage(temp, 0, 0, null);
        g.dispose();

        // Soften.
        float softenFactor = 0.05f;
        float[] softenArray = {0, softenFactor, 0, softenFactor, 1 - (softenFactor * 4), softenFactor, 0, softenFactor, 0};
        Kernel kernel = new Kernel(3, 3, softenArray);
        ConvolveOp cOp = new ConvolveOp(kernel, ConvolveOp.EDGE_NO_OP, null);
        bufferedImage = cOp.filter(bufferedImage, null);

        return bufferedImage;
    }    
    

}
