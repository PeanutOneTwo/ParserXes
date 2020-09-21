package ParserXml.ParserXml;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.crypto.Data;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.tree.model.InformationGainStats;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Size;
import org.apache.spark.sql.connector.expressions.Lit;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.opencypher.relocated.org.atnos.eff.syntax.list;

import com.google.inject.TypeLiteral;
import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import breeze.linalg.trace;
import dk.brics.automaton.Datatypes;
import scala.collection.Seq;

public class ParserXesUniversale {
	
	
	
    
    public static void main(String[] args) throws AnalysisException {
        
    	
    	
        
        /*
         *  Need to pass two args: 
         *  1: DbUrl
         *  2: Filepath
         */
    	
	    long Tempo1;
        long Tempo2;
        long Tempo;
        
        Tempo1=System.currentTimeMillis();
        
        String dbUrl= args[0];
        String PathFile = args[1];
        
        
        Properties connectionProperties = new Properties();
            connectionProperties.put("user", "root");
            connectionProperties.put("password", "");   
    
            
    	Boolean Intero =  false;
    	Boolean Stringa =  false;
    	Boolean Floatt =  false;
    	Boolean Date =  false;
    	Boolean Booolean  =  false ; 
    	Boolean ID = false; 
    	Boolean Container =  false; 
    	Boolean List = false; 
    	
    	
    	
        
      SparkConf conf = new SparkConf().setAppName("ParseFileXes").setMaster("local").set("spark.testing.memory", "2147480000");
      JavaSparkContext sc = new JavaSparkContext(conf);
      

      String prova = "_openxes.version";
      
      SparkSession spark = SparkSession
              .builder()
              .appName("ParseFileXes")
              .config(conf)
              .getOrCreate();
      
      
      
      
      spark.sql("set spark.sql.caseSensitive=true");
      

      /*
       * Carichiamo il file e stabiliamo come tag principale "trace". Teniamo conto che le informazioni contenute nella tag "log" non ci interessano.  
       * Ad ogni nuova modifica del dataset, si farà il printschema per capire meglio le modifiche apportate 
       */
      
      
      
      Dataset<Row> dftrace = spark.read().format("xml")
              .option("rowTag", "trace")
              .load(PathFile);
      
      
      System.out.println("dftrace SCHEMA");
      dftrace.printSchema();

      
      
      
      /*
       * 
       * 
       * Le variabili booleane servono per capire se l'attributo è presente all'interno della struttura del file.  
       * Una volta confermato che l'attributo è presente, controllo il tipo di struttura e nel caso sia di tipo array, lo scompongo con la funzione explode() 
       * Questo procedimento serve per armonizzare la struttura e rendere pi� facile l'inserimento sul database. 
       * 
       */
               
      
      Intero =  checkAttributes(dftrace, "int"); 
      Stringa =  checkAttributes(dftrace, "string"); 
      Floatt =  checkAttributes(dftrace, "float"); 
      Date =  checkAttributes(dftrace, "date"); 
      Booolean =  checkAttributes(dftrace, "boolean");
      Container =  checkAttributes(dftrace, "container"); 
      List =  checkAttributes(dftrace, "list"); 
      


      //Variabile di appoggio 
      Dataset<Row> datasetpulito = dftrace; 
      
      
      /*
       * In questa parte del codice cerco di semplificare la struttura del dataset per permette una facile estrazione dei valori che mi interessano. Per ogni attributo controllo che sia presente all'interno dello schema.
       * Una volta che ho la certezza che sia presente, controllo se sia un array. Se lo �, lo scoppio e lo inserisco all'interno del datasetpulito. Senn� carico direttamente la colonna cos� come � presente all'interno del dataset 
       * di origine. 
     */

       if (Stringa) {
    	  if (IsArray(dftrace, "string")) {
    		  Column Arraystring  =  org.apache.spark.sql.functions.explode(dftrace.col("string")).as("string"); 
    		  datasetpulito =  dftrace.withColumn("String", Arraystring); 
    		  datasetpulito =  datasetpulito.drop("string");
    	  }else {
    		  Column stringnormale = dftrace.col("string"); 
    		  datasetpulito = dftrace.withColumn("String", stringnormale); 
    		  datasetpulito = datasetpulito.drop("string");
		}   	  
      }
        
      
      if (Intero) {
    	  if (IsArray(dftrace, "int")) {
    		Column Arrayint = org.apache.spark.sql.functions.explode(dftrace.col("int")).as("int") ;   
    		datasetpulito =  datasetpulito.withColumn("Int", Arrayint);
    		datasetpulito = datasetpulito.drop("int"); 
			
		}else {
			
			datasetpulito =  datasetpulito.withColumn("Int", dftrace.col("int")); 
			datasetpulito = datasetpulito.drop("int"); 
			
		}	 	  
	}
      
      
     if (Floatt) {
		
    	 if (IsArray(dftrace, "float")) {
    		 Column Arrayfloat =  org.apache.spark.sql.functions.explode(dftrace.col("float")).as("float");
    		 datasetpulito =  datasetpulito.withColumn("Float", Arrayfloat); 
    		 datasetpulito = datasetpulito.drop("float");    
			
		}else {
			datasetpulito = datasetpulito.withColumn("Float", dftrace.col("float")); 
			datasetpulito =  datasetpulito.drop("float"); 
		}
     }
    	 
    	 
    	 
     if (Date) {
			if (IsArray(dftrace, "date")) {
				Column arraydate =  org.apache.spark.sql.functions.explode(dftrace.col("date")).as("date");
				datasetpulito = datasetpulito.withColumn("Date", arraydate);
				datasetpulito = datasetpulito.drop("date"); 		
			}else {
				datasetpulito = datasetpulito.withColumn("Date", dftrace.col("date")); 
				datasetpulito = datasetpulito.drop("date"); 
			}
		}
    	
    	 
    if (Booolean) {
		if (IsArray(dftrace, "boolean")) {
			Column booleanArray = org.apache.spark.sql.functions.explode(dftrace.col("boolean")).as("boolean");
			datasetpulito =  datasetpulito.withColumn("Boolean", booleanArray); 
			datasetpulito = datasetpulito.drop("boolen");
		}else {
			datasetpulito = datasetpulito.withColumn("Boolean", dftrace.col("boolean")); 
			datasetpulito = datasetpulito.drop("boolean"); 		
		}
		
	}
    
    
    Dataset<Row> tracelog =  datasetpulito; 
    
    if (Stringa) {
		tracelog = tracelog.withColumn("traceString_key", datasetpulito.col("String._key")); 
		tracelog = tracelog.withColumn("traceString_value", datasetpulito.col("String._value")); 
		tracelog = tracelog.drop("String");
	}
    
    if (Intero) {
		tracelog = tracelog.withColumn("traceInt_key", datasetpulito.col("Int._key")); 
		tracelog = tracelog.withColumn("traceInt_value", datasetpulito.col("Int._value")); 
		tracelog = tracelog.drop("Int");
	}
    
    if (Floatt) {
		tracelog = tracelog.withColumn("traceFloat_key", datasetpulito.col("Float._key")); 
		tracelog = tracelog.withColumn("traceFloat_value", datasetpulito.col("Float._value")); 
		tracelog = tracelog.drop("Float");
	}
    
    if (Booolean) {
		tracelog = tracelog.withColumn("TraceBoolean_key", datasetpulito.col("Boolean._key")); 
		tracelog = tracelog.withColumn("TraceBoolean_value", datasetpulito.col("Boolean._value")); 
		tracelog = tracelog.drop("Boolean");
	}
    
    if (Date) {
		tracelog = tracelog.withColumn("TraceDate_key", datasetpulito.col("Date._key")); 
		tracelog = tracelog.withColumn("TraceDate_value", datasetpulito.col("Date._value")); 
		tracelog = tracelog.drop("Date");
	}
    
    tracelog =  tracelog.drop("event"); 
    
    System.out.println("tracelog SCHEMA");

    String name = "concept:name"; 
    
    long numerotrace = tracelog.select(tracelog.col("traceString_value")).where(tracelog.col("traceString_key").equalTo(name)).count();
    
    tracelog.printSchema();
    
    DataFrameWriter<Row> trace =  new DataFrameWriter<Row>(tracelog);
    trace.mode(SaveMode.Overwrite);
    trace.jdbc(dbUrl, "trace", connectionProperties);

    
      
     
      
      //---------------------------------------------------Fase event------------------------------------
      
     /*
      * In questa fase, ripeto lo stesso procedimento fatto per la tag "trace". 
      * Controllo tutte le tipologie di attributo e semplifico la struttura.  
      * Non sappiamo quanti tipologie di attributi abbiamo. Quindi devo analizzarli tutti quanti. 
      * Ho creato un dataset che contiene tutte le stringhe della traccia che contiene tutti gli eventi. 
      * 
      */

    
    
    Dataset<Row> listaeventi =  datasetpulito.select(datasetpulito.col("String._value"), org.apache.spark.sql.functions.explode(dftrace.col("event")).as("event")); //Prima di tutto devo explodere event.
    
    Dataset<Row> DsSupporto =  listaeventi;
    
    DsSupporto.printSchema();
    
	Boolean InteroEvent =  false;
	Boolean StringaEvent =  false;
	Boolean FloattEvent =  false;
	Boolean DateEvent =  false;
	Boolean BoooleanEvent  =  false ; 
	Boolean IDEvent = false; 
	Boolean ContainerEvent =  false; 
	Boolean ListEvent = false; 
    
	InteroEvent =  checkAttributes(listaeventi, "event.int"); 
	StringaEvent =  checkAttributes(listaeventi, "event.string"); 
	FloattEvent =  checkAttributes(listaeventi, "event.float"); 
	DateEvent =  checkAttributes(listaeventi, "event.date"); 
	BoooleanEvent =  checkAttributes(listaeventi, "event.boolean");
	ContainerEvent =  checkAttributes(listaeventi, "event.container"); 
	ListEvent =  checkAttributes(listaeventi, "event.list"); 
	
	
	/*
	 * In questa parte controllo se gli attributi sono degli array.
	 */
    
    if (StringaEvent) {
    	if (IsArray(DsSupporto, "event.string")) {
    		Column appoggio =  listaeventi.col("event.string"); 
    		appoggio =  org.apache.spark.sql.functions.explode(appoggio);
    		DsSupporto = DsSupporto.withColumn("EventString", appoggio);  
		
    	}else {
			
			DsSupporto = DsSupporto.withColumn("EventString", listaeventi.col("event.string").as("EventString")); 
		}
	}
    
    if (DateEvent) {
    	
    	Column appoggio =  listaeventi.col("event.date"); 
    	
    	if (IsArray(DsSupporto, "event.date")) {
			 
    		appoggio =  org.apache.spark.sql.functions.explode(appoggio);
    		DsSupporto = DsSupporto.withColumn("EventDate", appoggio);     
    	    
		}else {
			DsSupporto =  DsSupporto.withColumn("EventDate", listaeventi.col("event.date")); 
		}
	}
    
    if (FloattEvent) {
    	
    	Column appoggio =  listaeventi.col("event.float"); 
    	
    	if (IsArray(DsSupporto, "event.float")) {
			 
    		appoggio =  org.apache.spark.sql.functions.explode(appoggio);
    		DsSupporto = DsSupporto.withColumn("EventFloat", appoggio);     
    	    
		}else {
			DsSupporto =  DsSupporto.withColumn("EventFloat", listaeventi.col("event.float")); 
		}
	}
    
    if (BoooleanEvent) {
    	if (IsArray(DsSupporto, "event.boolean")) {
    		Column appoggio =  listaeventi.col("event.boolean"); 
    		appoggio =  org.apache.spark.sql.functions.explode(appoggio);
    		DsSupporto = DsSupporto.withColumn("EventBoolean", appoggio);  
		
    	}else {
			
			DsSupporto = DsSupporto.withColumn("EventBoolean", listaeventi.col("event.boolean")); 
		}
	}
    
    if (InteroEvent) {
    	if (IsArray(DsSupporto, "event.int")) {
    		Column appoggio =  listaeventi.col("event.int"); 
    		appoggio =  org.apache.spark.sql.functions.explode(appoggio);
    		DsSupporto = DsSupporto.withColumn("EventInt", appoggio);  
		
    	}else {
			
			DsSupporto = DsSupporto.withColumn("EventInt", listaeventi.col("event.int")); 
		}
	}
    
    DsSupporto = DsSupporto.drop("event");
   

    /*
     * In questa parte vado a prendere la coppia chiave valore  degli attributi.
     */
    
    Dataset<Row> traceeventlog =  DsSupporto; 
    
    if (StringaEvent) {
		traceeventlog = traceeventlog.withColumn("EventString_key", DsSupporto.col("EventString._key")); 
		traceeventlog =  traceeventlog.withColumn("EventString_value", DsSupporto.col("EventString._value")); 
		traceeventlog =  traceeventlog.drop("EventString"); 
	}
    
    if (DateEvent) {
		traceeventlog = traceeventlog.withColumn("EventDate_key", DsSupporto.col("EventDate._key")); 
		traceeventlog =  traceeventlog.withColumn("EventDate_value", DsSupporto.col("EventDate._value")); 
		traceeventlog =  traceeventlog.drop("EventDate"); 
		
	}
    
    DsSupporto.printSchema();
    if (FloattEvent) {
		traceeventlog = traceeventlog.withColumn("EventFloat_key", DsSupporto.col("EventFloat._key")); 
		traceeventlog =  traceeventlog.withColumn("EventFloat_value", DsSupporto.col("EventFloat._value")); 
		traceeventlog =  traceeventlog.drop("EventFloat"); 
		
	}
    
    if (InteroEvent) {
		traceeventlog = traceeventlog.withColumn("EventInt_key", DsSupporto.col("EventInt._key")); 
		traceeventlog =  traceeventlog.withColumn("EventInt_value", DsSupporto.col("EventInt._value")); 
		traceeventlog =  traceeventlog.drop("EventInt"); 
		
	}
    
    
    
    if (BoooleanEvent) {
		traceeventlog = traceeventlog.withColumn("EventBoolean_key", DsSupporto.col("EventBoolean._key")); 
		traceeventlog =  traceeventlog.withColumn("EventBoolean_value", DsSupporto.col("EventBoolean._value")); 
		traceeventlog =  traceeventlog.drop("EventBoolean"); 
		
	}
      

    System.out.println("SCHEMA PULITO DAGLI ARRAY");
    traceeventlog.printSchema();

    
    /*
     * In questa parte, andrò a dare una riordinata alla montagna di dati che abbiamo. 
     * Ipotizziamo che gli eventi abbiano pochi attributi e che sono ripetuti nella stessa struttura. 
     * Questo ci permette di utilizzare un metodo ricorsivo per tutti gli attributi e riuscire a minimizzare il numero degli inserimenti sul database, che nel caso peggiore sarebbero aumentati
     * in base al numero di attributi che ciascun evento ha. 
     * 
     */
    
    
    Dataset<Row> eventids = traceeventlog; 
    Dataset<Row> traceIDColumnsDataset =  traceeventlog.select(traceeventlog.col("_value")).distinct(); 
    Dataset<Row> datasetpulitissimo =  traceeventlog.select(traceeventlog.col("_value")).distinct();
    
    
    /*
     * TEST: Il with column funziona un pò come cazzo gli pare. Non vuole gli stessi nomi ma la stessa struttura
     * 
     * Purtroppo non è possibile riuscire a fare un inserimento pulito di tutto il file poichè quando si và a fare il join, si andrebbe a moltiplicare per troppe volte il prodotto. 
     * Esistono due possibili soluzioni a questo problema: 
     *  -Per ogni nome dell'attributo stringa, si fà il join cosi da avere un ordinamento   e poi inserire la colonna in un dataset che contiene inizialmente solo i traceID e poi man manco anche le colonne dei vari attributi. 
     *  Il problema di questa soluzione è che il metodo withcolumn crea problemi che non riesco a risolvere. 
     *  
     *  -Si propone dei join multipli. Questa soluzione ha due principali problemi: il primo di natura computazionale a grandi quantità di attributi, il secondo richiede di salvarsi tutti i vari valori degli attributi
     */
    
    /*
    if (StringaEvent) {
    	
		 Dataset<Row> attributostringaevento =  eventids.select(eventids.col("EventString_key")).distinct(); 
		 List<Row> listaattributistringa = attributostringaevento.collectAsList();
		 for (Row row : listaattributistringa) {
			 
			 	String nomeattributo  = row.getString(0); //Abbiamo il nome dell'attributo. Io adesso voglio prendere tutti i valori che ha questo attributo.
			 	
			 	Dataset<Row> appoggio =  eventids.select(eventids.col("_value").as("TraceId"), eventids.col("EventString_value").as(nomeattributo)).where(eventids.col("EventString_key").contains(nomeattributo)); //Questo dataset contiene tutti i valori dell'attributo preso in questione. Quindi adesso devo aggiungerlo ad eventi ids mettendo come condizione che i due trace id devono essere uguali.  		 	
			 	Dataset<Row> appoggio2 =  traceIDColumnsDataset.join(appoggio, traceIDColumnsDataset.col("_value").equalTo(appoggio.col("TraceId")));
			 	appoggio2.drop(appoggio2.col("TraceId"));
			 	
			 	appoggio2 =  appoggio2.select(appoggio2.col(nomeattributo).as("congiunzione")); 			 	
			 	StructType schemadataset =  datasetpulitissimo.schema();
			 	schemadataset= schemadataset.add(nomeattributo, DataTypes.StringType, true); 
			 	
			 	Dataset<Row> possente =  spark.createDataFrame(appoggio2.javaRDD(), schemadataset); 
			 	
			 	possente =  possente.withColumn("ciaone", appoggio2.col("congiunzione"));

			 	
			 	datasetpulitissimo  =  possente; 
		}
		 
		 datasetpulitissimo =  datasetpulitissimo.drop(datasetpulitissimo.col("EventString_key")); 
		
	}
	
    
    
    if (FloattEvent) {
    	
		 Dataset<Row> attributofloatevento =  eventids.select(eventids.col("EventFloat_key")).distinct(); 
		 List<Row> listaattributifloat = attributofloatevento.collectAsList();
		 for (Row row : listaattributifloat) {
			 	String nomeattributo  = row.getString(0); //Abbiamo il nome dell'attributo. Io adesso voglio prendere tutti i valori che ha questo attributo. 
			 	Dataset<Row> appoggio =  eventids.select(eventids.col("EventFloat_value").as(nomeattributo), eventids.col("_value").as("TraceId")).where(eventids.col("EventFloat_key").contains(nomeattributo)); //Questo dataset contiene tutti i valori dell'attributo preso in questione. Quindi adesso devo aggiungerlo ad eventi ids mettendo come condizione che i due trace id devono essere uguali.  
			 	eventids=  eventids.join(appoggio, eventids.col("_value").equalTo(appoggio.col("TraceId"))); 	
		}
	}
    
    if (BoooleanEvent) {
    	
		 Dataset<Row> attributobooleanevento =  spark.sql("SELECT DISTINCT EventBoolean_key FROM evento"); 
		 List<Row> listaattributifloat = attributobooleanevento.collectAsList();
		 for (Row row : listaattributifloat) {
			 	String nomeattributo  = row.getString(0); 
				String query = "SELECT _value as TraceId,  EventBoolean_value  FROM evento WHERE evento.EventBoolean_key = '"+nomeattributo+"'"; 
				Dataset<Row> appoggio2 =  spark.sql(query); 
				appoggio2 = appoggio2.select(appoggio2.col("TraceId"), appoggio2.col("EventBoolean_value").as(nomeattributo)); 
				eventids =  eventids.join(appoggio2, eventids.col("_value").equalTo(appoggio2.col("TraceId"))); 			
		}
	}
    
    if (DateEvent) {
        
        Dataset<Row> attributofloatevento =   eventids.select(eventids.col("EventDate_key")).distinct();
        List<Row> listaattributifloat = attributofloatevento.collectAsList();
        for (Row row : listaattributifloat) {
               String nomeattributo  = row.getString(0); 
               Dataset<Row> appoggio2 = eventids.select(eventids.col("TraceId"), eventids.col("EventDate_value").as(nomeattributo)); 
               appoggio2 = appoggio2.select(appoggio2.col("TraceId"), appoggio2.col("EventDate_value").as(nomeattributo)); 
               eventids =  eventids.join(appoggio2, eventids.col("_value").equalTo(appoggio2.col("TraceId")));             
       }
   }
   
   if (InteroEvent) {
       
        Dataset<Row> attributointeroevento = eventids.select(eventids.col("EventInt_key")).distinct();
        List<Row> listaattributifloat = attributointeroevento.collectAsList();
        for (Row row : listaattributifloat) {
               String nomeattributo  = row.getString(0); 
               Dataset<Row> appoggio2 = eventids.select(eventids.col("TraceId"), eventids.col("EventInt_value").as(nomeattributo)); 
               appoggio2 = appoggio2.select(appoggio2.col("TraceId"), appoggio2.col("EventInt_value").as(nomeattributo)); 
               eventids =  eventids.join(appoggio2, eventids.col("_value").equalTo(appoggio2.col("TraceId")));             
       }
   }
    
    
    
    
    
	eventids =  eventids.drop("TraceId");
    */
    
    
    
	System.out.println("SCHEMA finale evento");
	eventids.printSchema();
	
	
    
	Long numeroeventi =  eventids.select("EventString_value").where(eventids.col("EventString_key").equalTo(name)).count(); 
	DataFrameWriter<Row> event =  new DataFrameWriter<Row>(eventids); 

	event.mode(SaveMode.Overwrite); 
	event.jdbc(dbUrl, "event", connectionProperties);
	
    Tempo2=System.currentTimeMillis();
    Tempo =  Tempo2 - Tempo1; 
    
    
    System.out.println("Il numero totale delle tracce � "+numerotrace);
	System.out.println("Il numero di eventi totali � "+numeroeventi);	
    System.out.println(Tempo);

          
    }
    
    
    public static Boolean isArrayColumn (Column colonna) {
    	
    	try {
    		org.apache.spark.sql.functions.explode(colonna);
    		return true; 
    		
		} catch (Exception e) {
			return false; 
		}
		
	}
       
    public static Boolean FindColumns(Dataset<Row> Ds, String Columns) {
    	
    	try {
			Ds.col(Columns); 
			return true; 
		} catch (Exception e) {
			return false; 
		}
    	
		
	}
    
    public static Boolean IsArray(Dataset<Row> Ds, String Columns) {
    	try { 
    	Ds.select(org.apache.spark.sql.functions.explode(Ds.col(Columns)));
    	return true;
		} catch (Exception e) {
			return false; 
		}
		
	}

    public static Boolean checkAttributes(Dataset<Row> Ds, String Attributes) {
    	try {
			Ds.col(Attributes); 
			return true; 
		} catch (Exception e) {
			return false; 
		}
		
	}
    
    public static String escapeStringForMySQL(String s) {
        return s.replaceAll(":", "\\:");
    }
}
