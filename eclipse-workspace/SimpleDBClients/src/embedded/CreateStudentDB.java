package embedded;
import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class CreateStudentDB {
	public static void main(String[] args) {
		Driver d = new EmbeddedDriver();
		String url = "jdbc:simpledb:studentdb";

		try (Connection conn = d.connect(url, null);
				Statement stmt = conn.createStatement()) {
			String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			stmt.executeUpdate(s);
			System.out.println("Table STUDENT created.");

			s = "create index idx_majorid on STUDENT(MajorId) using hash";
			stmt.executeUpdate(s);
			System.out.println("Index on MajorId created.");

			s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			String[] studvals = {
					"(1, 'joe', 10, 2021)",
					"(2, 'amy', 20, 2020)",
					"(3, 'max', 40, 2022)",
					"(4, 'sue', 20, 2022)",
					"(5, 'bob', 30, 2020)",
					"(6, 'kim', 20, 2020)",
					"(7, 'art', 30, 2021)",
					"(8, 'pat', 40, 2019)",
					"(9, 'lee', 10, 2021)",
					"(10, 'joey', 50, 2021)",
					"(11, 'army', 20, 2020)",
					"(12, 'maxin', 30, 2022)",
					"(13, 'sued', 10, 2022)",
					"(14, 'bobby', 30, 2020)",
					"(15, 'kimer', 40, 2018)",
					"(16, 'pim', 30, 2020)",
					"(17, 'tim', 20, 2017)",
					"(18, 'leed', 40, 2018)",
					"(19, 'joed', 10, 2018)",
					"(20, 'amed', 20, 2019)",
					"(21, 'mex', 10, 2022)",
					"(22, 'see', 50, 2022)",
					"(23, 'bubble', 50, 2021)",
					"(24, 'kimber', 50, 2020)",
					"(25, 'arter', 30, 2021)",
					"(26, 'patter', 20, 2022)",
					"(27, 'jordan', 30, 2021)",
					"(28, 'gordon', 10, 2015)",
					"(29, 'james', 40, 2020)",
					"(30, 'michael', 50, 2022)",
					"(31, 'susan', 20, 2022)",
					"(32, 'josh', 30, 2020)",
					"(33, 'kin', 20, 2020)",
					"(34, 'yoke', 30, 2021)",
					"(35, 'donald', 20, 2019)",
					"(36, 'kisuke', 10, 2021)",
					"(37, 'tommy', 50, 2022)",
					"(38, 'hardy', 50, 2021)",
					"(39, 'luffy', 50, 2020)",
					"(40, 'zoro', 30, 2021)",
					"(41, 'sanji', 20, 2022)",
					"(42, 'brook', 30, 2021)",
					"(43, 'chopper', 10, 2015)",
					"(44, 'robin', 40, 2020)",
					"(45, 'nami', 50, 2022)",
					"(46, 'momo', 20, 2022)",
					"(47, 'yamato', 30, 2020)",
					"(48, 'eagle', 20, 2020)",
					"(49, 'thomson', 30, 2021)",
					"(50, 'kayne', 20, 2019)",

			};
			for (int i=0; i<studvals.length; i++)
				stmt.executeUpdate(s + studvals[i]);
			System.out.println("STUDENT records inserted.");

			s = "create table DEPT(DId int, DName varchar(8))";
			stmt.executeUpdate(s);
			System.out.println("Table DEPT created.");

			s = "insert into DEPT(DId, DName) values ";
			String[] deptvals = {
					"(10, 'compsci')",
					"(20, 'math')",
					"(30, 'drama')",
					"(40, 'history')",
					"(50, 'music')"
			};
			for (int i=0; i<deptvals.length; i++)
				stmt.executeUpdate(s + deptvals[i]);
			System.out.println("DEPT records inserted.");

			s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
			stmt.executeUpdate(s);
			System.out.println("Table COURSE created.");

			s = "insert into COURSE(CId, Title, DeptId) values ";
			String[] coursevals = {
					"(12, 'db systems', 10)",
					"(22, 'compilers', 10)",
					"(32, 'calculus', 20)",
					"(42, 'algebra', 20)",
					"(52, 'acting', 40)",
					"(62, 'elocution', 30)",
					"(72, 'driving', 20)",
					"(82, 'fencing', 10)",
					"(92, 'opera', 50)",
					"(102, 'astronomy', 20)",
					"(112, 'electrical', 30)",
					"(122, 'music', 40)",
					"(132, 'computing', 10)",
					"(142, 'handbell', 10)",
					"(152, 'alchemy', 20)",
					"(162, 'science', 20)",
					"(172, 'politics', 40)",
					"(182, 'evolution', 30)",
					"(192, 'drama', 20)",
					"(202, 'swimming', 10)",
					"(212, 'dancing', 50)",
					"(222, 'accounting', 20)",
					"(232, 'teaching', 30)",
					"(242, 'badminton', 40)",
					"(252, 'league', 10)",
					"(262, 'dota', 10)",
					"(272, 'games', 20)",
					"(282, 'food science', 20)",
					"(292, 'animal', 40)",
					"(302, 'MDP', 30)"
			};
			for (int i=0; i<coursevals.length; i++)
				stmt.executeUpdate(s + coursevals[i]);
			System.out.println("COURSE records inserted.");

			s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
			stmt.executeUpdate(s);
			System.out.println("Table SECTION created.");

			s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
			String[] sectvals = {
					"(13, 12, 'turing', 2018)",
					"(23, 12, 'turing', 2019)",
					"(33, 32, 'newton', 2019)",
					"(43, 32, 'einstein', 2017)",
					"(53, 62, 'brando', 2018)",
					"(63, 92, 'lifang', 2018)",
					"(73, 82, 'loke', 2019)",
					"(83, 102, 'jackie', 2019)",
					"(93, 112, 'einstein', 2017)",
					"(103, 62, 'brando', 2018)",
					"(113, 142, 'turing', 2018)",
					"(123, 132, 'turing', 2019)",
					"(133, 32, 'newton', 2019)",
					"(143, 32, 'loke', 2017)",
					"(153, 62, 'brando', 2018)",
					"(163, 52, 'loke', 2018)",
					"(173, 12, 'tomson', 2018)",
					"(183, 292, 'lifang', 2019)",
					"(193, 242, 'newton', 2019)",
					"(203, 302, 'einstein', 2017)"
			};
			for (int i=0; i<sectvals.length; i++)
				stmt.executeUpdate(s + sectvals[i]);
			System.out.println("SECTION records inserted.");

			s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
			stmt.executeUpdate(s);
			System.out.println("Table ENROLL created.");

			s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
			String[] enrollvals = {
					"(14, 1, 13, 'A')",
					"(24, 1, 43, 'C' )",
					"(34, 2, 43, 'B+')",
					"(44, 4, 33, 'B' )",
					"(54, 4, 53, 'A' )",
					"(64, 6, 53, 'A' )"
			};
			for (int i=0; i<enrollvals.length; i++)
				stmt.executeUpdate(s + enrollvals[i]);
			System.out.println("ENROLL records inserted.");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	}
}
