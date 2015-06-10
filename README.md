# Storm-WordCount
A storm example - WordCount - Modified


1. how to install : http://www.thecloudavenue.com/2013/11/InstallingAndConfiguringStormOnUbuntu.html

2. set path variables:

	export PATH=$PATH:/home/gggopi/apache-storm-0.9.5/bin
	export PATH=$PATH:/home/gggopi/apache-maven-3.3.3/bin

3. Run Commands:

	mvn clean install

	storm jar ./target/myProj-1.0-SNAPSHOT.jar com.tataatsu.WordCountTopology >abc.txt

4. Check abc.txt for the output!!

