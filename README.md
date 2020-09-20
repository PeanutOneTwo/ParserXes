Schema readme
-Abstract: spieghiamo cosafà in linea generale e il perchè è stato sviluppato
-File .xes: Spieghiamo cosa sono e linkiamo la pagina dell'università di Eindhoven
-Parser: Struttura del software 
-Run configuration: Mettiamo screen di come settare la configurazione e del risultato prodotto



Introduzione
--------
Questo parser serve per estrapolare le informazioni contenute all'interno dei file con estensione .xes. Il programma, una volta caricato il file, cerca le tag principali "trace" e "event",  prende il valore dei suoi attibuti e li inserisce all'interno di un database indicato. 

File xes
--------
XES is an XML-based standard for event logs. Its purpose is to provide a generally-acknowledged format for the interchange of event log data between tools and application domains. Its primary purpose is for process mining, i.e. the analysis of operational processes based on their event logs. However, XES has been designed to also be suitable for general data mining, text mining, and statistical analysis.
The UML 2.0 class diagram describes the complete meta-model for the XES standard.

![schemaxes](https://github.com/PeanutOneTwo/ParserXes/blob/master/images/Cattura.PNG)

For other informations, you can go [here](https://research.tue.nl/en/publications/xes-standard-definition)
