
-Run configuration: Mettiamo screen di come settare la configurazione e del risultato prodotto



Introduzione
--------
ParserXES è stato sviluppato in seguito alla necessità di avere un tool che permettesse di caricare all'interno di un database file di notevoli dimensioni. Il tool è stato sviluppato in java utilizzando il modulo [SparkSQL](https://spark.apache.org/sql/) di Apache Spark. L'utilizzo di questo framework ha permesso sia di migliorare notevolmente le prestazioni del tool all'aumentare del carico di lavoro, sia di utilizzare strutture dati che si prestano molto bene al focus del progetto. 

File xes
--------
XES is an XML-based standard for event logs. Its purpose is to provide a generally-acknowledged format for the interchange of event log data between tools and application domains. Its primary purpose is for process mining, i.e. the analysis of operational processes based on their event logs. However, XES has been designed to also be suitable for general data mining, text mining, and statistical analysis.
The UML 2.0 class diagram describes the complete meta-model for the XES standard.

![schemaxes](https://github.com/PeanutOneTwo/ParserXes/blob/master/images/Cattura.PNG)

For other informations, you can go [here](https://research.tue.nl/en/publications/xes-standard-definition)


Struttura del codice
--------
Il codice si compone principalmente in due sezioni: 

* Nella prima sezione, si prenderanno tutti gli attributi della tag "trace" e "armonizzeremo" la loro struttura cosi da poterle caricare nella tabella "trace" del database indicato. 

* Nella seconda, si ripeterà lo stesso procedimento della prima sezione applicato sulla tag "event" e caricato all'interno della tabella "event". 


![Schema armonizzazione](https://github.com/PeanutOneTwo/ParserXes/blob/master/images/schemaarmonizzazione.PNG)

Run Configuration
--------
Bisogna passare due parametri:
 
 * Il path del database
 
 * Il path del file
 
 ![Esempio di Run configuration](https://github.com/PeanutOneTwo/ParserXes/blob/master/images/Run%20configuration.PNG)


