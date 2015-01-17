import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;


public class Hw2 {
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		String querryType=args[0];
		int switchcase=0;
		if(querryType.compareToIgnoreCase("window")==0)
			switchcase=1;
		else if (querryType.compareToIgnoreCase("within")==0)
			switchcase=2;
		else if (querryType.compareToIgnoreCase("nearest-neighbor")==0)
			switchcase=3;
		else if (querryType.compareToIgnoreCase("fixed")==0)
			switchcase=4;

		switch(switchcase)
		{
		case 1:
			String object=args[1];
			int[] coordinates=new int[4];
			coordinates[0]=Integer.parseInt(args[2]);
			coordinates[1]=Integer.parseInt(args[3]);
			coordinates[2]=Integer.parseInt(args[4]);
			coordinates[3]=Integer.parseInt(args[5]);
			if(object.compareToIgnoreCase("student")==0)
			{
				Connection conn=getConnection();
				window(conn, "students", coordinates);
				conn.close();
			}
			else if (object.compareToIgnoreCase("tramstop")==0)
			{
				Connection conn=getConnection();
				window(conn, "tramstops", coordinates);
				conn.close();
			}
			else if (object.compareToIgnoreCase("building")==0)
			{
				Connection conn=getConnection();
				window(conn, "buildings", coordinates);
				conn.close();
			}
			break;
		case 2:
			Connection conn=getConnection();
			within(conn, args[1],Integer.parseInt(args[2]));
			conn.close();
			break;
		case 3:
			String obj=args[1];
			String building=args[2];
			int k=Integer.parseInt(args[3]);

			if(obj.compareToIgnoreCase("student")==0)
			{
				Connection con=getConnection();
				nn(con, "students", building, k);
				con.close();
			}
			else if (obj.compareToIgnoreCase("tramstop")==0)
			{
				Connection con=getConnection();
				nn(con, "tramstops", building, k);
				con.close();
			}
			else if (obj.compareToIgnoreCase("building")==0)
			{
				Connection con=getConnection();
				nn(con, "buildings",building , k);
				con.close();
			}
			break;

		case 4:
			switch(Integer.parseInt(args[1]))
			{
			case 1:
				Connection con=getConnection();
				fixed1(con);
				con.close();

				break;
			case 2:
				Connection con1=getConnection();
				fixed2(con1);
				con1.close();
				break;
			case 3: 
				Connection con2=getConnection();
				fixed3(con2);
				con2.close();
				break;
			case 4:
				Connection con3=getConnection();
				fixed4(con3);
				con3.close();
				break;
			case 5:
				Connection con4=getConnection();
				fixed5(con4);
				con4.close();
			}
			break;

		}
	}

	public static Connection getConnection() throws SQLException, ClassNotFoundException {

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", "SYSTEM");
		connectionProps.put("password", "qwerty");

		Class.forName ("oracle.jdbc.driver.OracleDriver");
		conn = DriverManager.getConnection(
				"jdbc:oracle:thin:@localhost:1521:orcl",
				connectionProps);
		System.out.println("Connected to database");
		return conn;
	}

	public static void window(Connection conn, String tablename, int[] coordinates) throws SQLException
	{
		Statement stmt = null;
		String query="";
		if(tablename.compareToIgnoreCase("students")==0)
		{
			query = "select A.sid " +
					"from " + tablename+" A WHERE sdo_filter(A.geom, SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,3),SDO_ordinate_array("+coordinates[0]+","+coordinates[1]+","+coordinates[2]+","+coordinates[3]+"))) = 'TRUE'"; 
		}
		else if(tablename.compareToIgnoreCase("tramstops")==0)
		{
			query = "select A.tramid " +
					"from " + tablename+" A WHERE sdo_filter(A.geom, SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,3),SDO_ordinate_array("+coordinates[0]+","+coordinates[1]+","+coordinates[2]+","+coordinates[3]+"))) = 'TRUE'"; 
		}
		else if(tablename.compareToIgnoreCase("buildings")==0)
		{
			query = "select A.bid, A.bname " +
					"from " + tablename+" A WHERE sdo_filter(A.geom, SDO_geometry(2003,NULL,NULL,SDO_elem_info_array(1,1003,3),SDO_ordinate_array("+coordinates[0]+","+coordinates[1]+","+coordinates[2]+","+coordinates[3]+"))) = 'TRUE'"; 
		}
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String sid="";
				if(tablename.compareToIgnoreCase("students")==0)
					sid = rs.getString("sid");
				else if (tablename.compareToIgnoreCase("tramstops")==0)
					sid = rs.getString("tramstops");
				else if(tablename.compareToIgnoreCase("buildings")==0)
				{sid=rs.getString("bid"); sid=sid+"  "+rs.getString("bname");}


				System.out.println(sid);
			}
		} catch (SQLException e ) {
			e.printStackTrace();
		} finally {
			if (stmt != null) { stmt.close(); }
		}

	}

	public static void within(Connection conn,String sid,int distance) throws SQLException
	{
		Statement stmt = null;
		Statement stmt1=null;
		String query="SELECT /*+ ORDERED */ T.TRAMID FROM STUDENTS S, TRAMSTOPS T WHERE S.SID='"+sid+"' AND SDO_WITHIN_DISTANCE(T.GEOM, S.GEOM, 'distance ="+distance+"')='TRUE' union SELECT /*+ ORDERED */ B.BID FROM STUDENTS S, BUILDINGS B WHERE S.SID='"+sid+"' AND SDO_WITHIN_DISTANCE(B.GEOM, S.GEOM, 'distance ="+distance+"')='TRUE'";
		System.out.println(query);

		try {
			stmt1=conn.createStatement();
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				String sid1=rs.getString("tramid");
				System.out.println(sid1);
			}

		} catch (SQLException e ) {
			e.printStackTrace();
		} finally {
			if (stmt != null) { stmt.close(); stmt1.close(); }
		}

	}

	public static void nn( Connection conn, String tablename, String bid, int n) throws SQLException
	{
		String tb="";
		String cl="";
		if(bid.charAt(0)=='b')
		{
			tb="Buildings";
			cl="bid";
		}
		else if(bid.charAt(0)=='t')
		{
			tb="Tramstops";
			cl="tramid";
		}
		else if(bid.charAt(0)=='p')
		{
			tb="Students";
			cl="sid";
		}

		String  query="";
		if (tablename.equalsIgnoreCase("students"))
		{
			query="SELECT SID FROM Students WHERE SDO_NN(GEOM,(SELECT GEOM FROM "+tb+" WHERE "+cl+" ='"+bid+"'),'sdo_num_res="+(n+1)+"') = 'TRUE' and sid!='"+bid+"'";
		}
		else if (tablename.equalsIgnoreCase("tramstops"))
		{
			query="SELECT tramid FROM tramstops WHERE SDO_NN(GEOM,(SELECT GEOM FROM "+tb+" WHERE "+cl+" ='"+bid+"'),'sdo_num_res="+(n+1)+"') = 'TRUE'and tramid!='"+bid+"'";
		}
		else if (tablename.equalsIgnoreCase("buildings"))
		{
			query="SELECT bid FROM buildings WHERE SDO_NN(GEOM,(SELECT GEOM FROM "+tb+" WHERE "+cl+" ='"+bid+"'),'sdo_num_res="+(n+1)+"') = 'TRUE' and bid!='"+bid+"'";
		}
		Statement stmt=null;
		stmt=conn.createStatement();
		ResultSet r=stmt.executeQuery(query);
		while(r.next())
		{
			if (tablename.equalsIgnoreCase("students"))
			{
				System.out.println(r.getString("sid"));
			}
			else if (tablename.equalsIgnoreCase("tramstops"))
			{
				System.out.println(r.getString("tramid"));
			}
			else if (tablename.equalsIgnoreCase("buildings"))
			{
				System.out.println(r.getString("bid"));
			}
		}

	}	

	private static void fixed1(Connection conn) throws SQLException
	{
		Statement stmt1=conn.createStatement();
		String query="(SELECT /*+ ORDERED */ S.SID FROM STUDENTS S, TRAMSTOPS T WHERE T.TRAMID='t2ohe' AND SDO_WITHIN_DISTANCE(T.GEOM, S.GEOM, 'distance =70')='TRUE' Union SELECT /*+ ORDERED */ B.BID FROM TRAMSTOPS T, BUILDINGS B WHERE T.TRAMID='t2ohe' AND SDO_WITHIN_DISTANCE(B.GEOM, T.GEOM, 'distance =70')='TRUE') Intersect (SELECT /*+ ORDERED */ S.SID FROM STUDENTS S, TRAMSTOPS T WHERE T.TRAMID='t6ssl' AND SDO_WITHIN_DISTANCE(T.GEOM, S.GEOM, 'distance =50')='TRUE' union SELECT /*+ ORDERED */ B.BID FROM TRAMSTOPS T, BUILDINGS B WHERE T.TRAMID='t6ssl' AND SDO_WITHIN_DISTANCE(B.GEOM, T.GEOM, 'distance =50')='TRUE')";
		ResultSet rs = stmt1.executeQuery(query);
		while (rs.next()) {
			System.out.println(rs.getString("sid"));
		}
	}

	private static void fixed2(Connection con1) throws SQLException {
		Statement stmt2=null;
		stmt2=con1.createStatement();

		String nearestTramStop="SELECT T.tramid, S.sid FROM Tramstops T, Students S WHERE SDO_NN(T.GEOM,S.GEOM,'sdo_num_res=2') = 'TRUE'";
		ResultSet rs2=stmt2.executeQuery(nearestTramStop);

		while(rs2.next())
		{
			System.out.print( rs2.getString("sid")+ " "+rs2.getString("tramid")+" ");
			System.out.println();
		}
		System.out.println();
	}


	private static void fixed3(Connection con2) throws SQLException {

		ArrayList<String> tramstops=new ArrayList<String>();
		ArrayList<Integer> number= new ArrayList<Integer>();
		
			String query="SELECT T.TRAMID, COUNT(B.BID) c FROM BUILDINGS B, TRAMSTOPS T WHERE SDO_WITHIN_DISTANCE(T.GEOM, B.GEOM, 'distance =250')='TRUE' GROUP BY T.TRAMID";
			Statement stmt1=con2.createStatement();
			ResultSet r1=stmt1.executeQuery(query);
			while(r1.next())
			{
				number.add(Integer.parseInt(r1.getString("c")));
				tramstops.add(r1.getString("tramid"));
			}
		
		int max=0;
		int index=0;
		for( int i=0; i<number.size();i++)
		{
			if(number.get(i)>=max)
			{
				max=number.get(i);
				index=i;
			}
		}
		System.out.println(tramstops.get(index)+" " + number.get(index));


	}

	private static void fixed4(Connection con3) throws SQLException {
		String query="SELECT * FROM (SELECT COUNT(BID) C,SID FROM BUILDINGS B, STUDENTS S WHERE SDO_NN(S.GEOM,B.GEOM,'sdo_num_res=1') = 'TRUE' GROUP BY sid ORDER BY COUNT(bid) DESC) WHERE ROWNUM < 6";
		Statement stmt=con3.createStatement();
		ResultSet r=stmt.executeQuery(query);
		while(r.next())
		{
			System.out.print(r.getString("sid") + "  ");
			System.out.print(r.getString("C"));
			System.out.println();
		}

	}

	private static void fixed5(Connection con4) throws SQLException {
		String query="SELECT SDO_GEOM.SDO_MIN_MBR_ORDINATE(GEOM,1) AS MINX, SDO_GEOM.SDO_MAX_MBR_ORDINATE(GEOM,1) AS MAXX, SDO_GEOM.SDO_MIN_MBR_ORDINATE(GEOM,2) AS MINY, SDO_GEOM.SDO_MAX_MBR_ORDINATE(GEOM,2) AS MAXY from (select  SDO_AGGR_MBR(geom) GEOM FROM buildings where bname like 'SS%') ";
		Statement stmt1=con4.createStatement();
		ResultSet r=stmt1.executeQuery(query);
		while(r.next())
		{
			System.out.println("Lower Left Coordinates of MBR : " + r.getString("MINX"));
			System.out.println("Lower Left Coordinates of MBR : " + r.getString("MINY"));
			System.out.println("Upper Right Coordinates of MBR : " + r.getString("MAXX"));
			System.out.println("Upper Right Coordinates of MBR : " + r.getString("MAXY"));
		}

	}

}
