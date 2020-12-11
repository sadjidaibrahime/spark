import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType,FloatType,DoubleType,IntegerType}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object App {
      
      def main(args: Array[String]):Unit={
        println("Hello World!")

         val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()

         val communes = spark.read.option("header",true).option("delimiter", ";").csv("/home/test/esgi_tp/Communes.csv")
         communes.show()
         communes.printSchema()

		 
	val synop = spark.read.option("header",true).option("delimiter", ";").csv("/home/test/esgi_tp/synop.2020120512.txt")
         synop.show()
         synop.printSchema()
		 
	 val code_insee = spark.read.option("header",true).option("delimiter", ";").csv("/home/test/esgi_tp/code-insee-postaux-geoflar.csv")
         code_insee.show()
         code_insee.printSchema()

         val postesSynop = spark.read.option("header",true).option("delimiter", ";").csv("/home/test/esgi_tp/postesSynop.txt")
         postesSynop.show()
         postesSynop.printSchema()



     }
}



