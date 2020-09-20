ParserXES
========================

Introduction
-------- 
ParserXES was developed following the need to have a tool *that would allow you to load large files into a database*. The tool was developed in java using the **Apache Spark [SparkSQL](https://spark.apache.org/sql/) module**. The use of this framework has allowed both to significantly improve the performance of the tool as the workload increases, and to use data structures that lend themselves very well to the focus of the project.


Library Versions
--------

*  **Spark** : spark-core_2.12-3.0.0-preview

*  **Java**: jdk-13.0.1

*  **Scala**: scala-library-2.12.10


Xes File
--------
XES is an XML-based standard for event logs. Its purpose is to provide a generally-acknowledged format for the interchange of event log data between tools and application domains. Its primary purpose is for process mining, i.e. the analysis of operational processes based on their event logs. However, XES has been designed to also be suitable for general data mining, text mining, and statistical analysis.
The UML 2.0 class diagram describes the complete meta-model for the XES standard.

![schemaxes](https://github.com/PeanutOneTwo/ParserXes/blob/master/images/Cattura.PNG)

For other informations, you can go [here](https://research.tue.nl/en/publications/xes-standard-definition)



Code Structure
--------

The code is mainly made up of **two sections**:

* In the first section, we will take all the attributes of the **"trace"** tag and "harmonize" their structure so that we can load them into the "trace" table of the database. 

* In the second, we will repeat the same procedure as in the first section applied to the **"event"** tag and loaded into the "event" table. 


![Schema armonizzazione](https://github.com/PeanutOneTwo/ParserXes/blob/master/images/schemaarmonizzazione.PNG)

Run Configuration
--------
Two parameters must be passed:
 
 * **The path to the database**
 
 * **the path of the file**
 
 ![Esempio di Run configuration](https://github.com/PeanutOneTwo/ParserXes/blob/master/images/Run%20configuration.PNG)


