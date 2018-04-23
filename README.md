# Start Apache Spark with Python

## Windows ##
1.	Install a JDK (Java Development Kit) from http://www.oracle.com/technetwork/java/javase/downloads/index.html . **You must install the 		JDK into a path with no spaces**, for example c:\jdk. Be sure to change the default location for the installation! **DO NOT INSTALL JAVA 9-INSTALL JAVA 8. Spark is not compatible with Java 9.**

2. 	Download a **pre-built** version of Apache Spark from https://spark.apache.org/downloads.html

3.	If necessary, download and install WinRAR so you can extract the .tgz file you downloaded. http://www.rarlab.com/download.htm

4.	Extract the Spark archive, and copy its **contents** into **C:\spark** after creating that directory. You should end up with 						directories like c:\spark\bin, c:\spark\conf, etc.

5.	Download winutils.exe from https://sundog–s3.amazonaws.com/winutils.exe and move it into a **C:\winutils\bin** folder that you’ve 			created. (note, this is a 64-bit application. If you are on a 32-bit version of Windows, you’ll need to search for a 32-bit build of 		 **winutils.exe** for Hadoop.)

6.	Open the the **c:\spark\conf** folder, and make sure “File Name Extensions” is checked in the “view” tab of Windows Explorer. Rename
		the **log4j.properties.template** file to **log4j.properties**. Edit this file (using Wordpad or something similar) and change the 			error level from **INFO to ERROR** for log4j.rootCategory

7.	Right-click your Windows menu, select Control Panel, System and Security, and then System. Click on “Advanced System Settings” and 
		then the “Environment Variables” button.

8.	Add the following new USER variables:
		
		1. **SPARK_HOME** c:\spark
		2. **JAVA_HOME** (the path you installed the JDK to in step 1, for example C:\JDK)
		3. **HADOOP HOME** c:\winutils

9.	Add the following paths to your PATH user variable:

		1.	**%SPARK_HOME%\bin**
		2.	**%JAVA_HOME%\bin**

10.	Close the environment variable screen and the control panels.

11.	Install the latest Enthought Canopy for Python 3.5 from https://store.enthought.com/downloads/#default Don’t install a Python 		2.7 version! **If you already have Python 3.5 don't need to install Canopy. Even you can intall Python differntly and 			configuret the same**

12. Test it out!

		1.	Open up Canopy and select “Canopy Command Prompt” from the Tools menu.
		2.	Enter cd c:\spark and then dir to get a directory listing.
		3.	Look for a text file we can play with, like README.md or CHANGES.txt
		4.	Enter pyspark
		5.	At this point you should have a >>> prompt. If not, double check the steps above.
		6.	Enter rdd = sc.textFile(“README.md”) (or whatever text file you’ve found) Enter rdd.count()
		7.	You should get a count of the number of lines in that file! Congratulations, you just ran your 
				first Spark program!
		8.	Enter quit() to exit the spark shell, and close the console window
		9.	You’ve got everything set up! Hooray!
		
## NOTE : ##
For dataset please download data from https://grouplens.org/datasets/movielens/ 
